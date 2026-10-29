//! Read an image out of the OS clipboard, cross-platform.
//!
//! Terminal paste events (`Event::Paste`) deliver *text* only — the terminal
//! emulator never ships the bytes of an image on paste. To get an image the
//! user has copied (or a screenshot they took) we have to leave the process
//! and ask the OS's clipboard directly:
//!
//! * **macOS** — `osascript`, writing `the clipboard as «class PNGf»` to a
//!   temp file. This is the same call `pngpaste` wraps, without the extra
//!   install; the AppleScript throws (non-zero exit) when the clipboard holds
//!   no image, which is exactly how we detect "nothing to paste".
//! * **Windows** — PowerShell with `System.Windows.Forms.Clipboard::GetImage()`,
//!   saved as PNG. `powershell.exe` ships with every supported Windows.
//! * **Linux** — try `wl-paste --type image/png` (Wayland) then
//!   `xclip -selection clipboard -t image/png -o` (X11). Both write the raw
//!   bytes to stdout and fail (or emit nothing) when the clipboard has no
//!   image.
//!
//! Each backend is a synchronous subprocess run on the event-loop thread. That
//! is a deliberate trade-off: a clipboard read is tens of milliseconds, and
//! the app has no async plumbing in `handle_key` to await one without a much
//! larger refactor. The one theoretical hazard is a Wayland compositor that
//! makes `wl-paste` block indefinitely waiting for a `--type` the clipboard
//! cannot supply; in practice `wl-paste --type image/png` returns immediately
//! with an error when no image is present.

use crate::llm::ImageAttachment;
use arboard::Clipboard;
use std::process::Command;

/// Encode RGBA bytes into PNG bytes in memory.
fn rgba_to_png(width: u32, height: u32, rgba: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut encoder = png::Encoder::new(&mut out, width, height);
    encoder.set_color(png::ColorType::Rgba);
    encoder.set_depth(png::BitDepth::Eight);
    let mut writer = encoder.write_header().ok()?;
    writer.write_image_data(rgba).ok()?;
    drop(writer);
    Some(out)
}

/// Optimize an image before staging/sending:
/// 1. If width or height exceeds 1600px, resize to fit 1600x1600 preserving aspect ratio.
/// 2. If the image is opaque, compress as high-quality JPEG (quality 85) to reduce payload size by 10-15x.
pub fn optimize_image(bytes: &[u8], preferred_mime: &str) -> (Vec<u8>, &'static str) {
    let Ok(mut img) = image::load_from_memory(bytes) else {
        return (
            bytes.to_vec(),
            if preferred_mime.contains("jpeg") || preferred_mime.contains("jpg") {
                "image/jpeg"
            } else {
                "image/png"
            },
        );
    };

    let (w, h) = (img.width(), img.height());
    if w > 1600 || h > 1600 {
        img = img.resize(1600, 1600, image::imageops::FilterType::Triangle);
    }

    let has_transparency = match &img {
        image::DynamicImage::ImageRgba8(rgba) => rgba.pixels().any(|p| p.0[3] < 250),
        image::DynamicImage::ImageRgba16(rgba) => rgba.pixels().any(|p| p.0[3] < 65000),
        _ => false,
    };

    if !has_transparency {
        let mut buf = std::io::Cursor::new(Vec::new());
        let rgb = img.to_rgb8();
        let mut encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut buf, 85);
        if encoder
            .encode(
                rgb.as_raw(),
                rgb.width(),
                rgb.height(),
                image::ExtendedColorType::Rgb8,
            )
            .is_ok()
        {
            let res = buf.into_inner();
            if !res.is_empty()
                && (res.len() < bytes.len()
                    || preferred_mime.contains("jpeg")
                    || preferred_mime.contains("jpg"))
            {
                return (res, "image/jpeg");
            }
        }
    }

    (
        bytes.to_vec(),
        if preferred_mime.contains("jpeg") || preferred_mime.contains("jpg") {
            "image/jpeg"
        } else {
            "image/png"
        },
    )
}

/// The image currently in the clipboard, base64-encoded as PNG or JPEG, or `None` if
/// the clipboard holds no image or the platform's clipboard tool is missing.
pub fn read_image() -> Option<ImageAttachment> {
    // 1. Try native in-memory clipboard first (instant, lag-free via arboard)
    if let Ok(mut cb) = Clipboard::new() {
        if let Ok(img) = cb.get_image() {
            if let Some(png_bytes) = rgba_to_png(img.width as u32, img.height as u32, &img.bytes) {
                if !png_bytes.is_empty() {
                    let (opt_bytes, mime) = optimize_image(&png_bytes, "image/png");
                    return Some(ImageAttachment {
                        mime_type: mime.to_string(),
                        data_base64: crate::backend::base64_encode(&opt_bytes),
                    });
                }
            }
        }
    }

    // 2. Platform subprocess fallback (e.g. headless/sandboxed sessions)
    #[cfg(target_os = "macos")]
    {
        return read_macos();
    }
    #[cfg(target_os = "windows")]
    {
        return read_windows();
    }
    #[cfg(target_os = "linux")]
    {
        return read_linux();
    }
    // Other targets have no supported clipboard tool.
    #[allow(unreachable_code)]
    None
}

/// The plain text currently in the clipboard, or `None` if the clipboard holds
/// no text or the platform tool is missing.
pub fn read_text() -> Option<String> {
    // 1. Try native in-memory clipboard first
    if let Ok(mut cb) = Clipboard::new() {
        if let Ok(text) = cb.get_text() {
            if !text.is_empty() {
                return Some(text);
            }
        }
    }

    // 2. Platform subprocess fallback
    #[cfg(target_os = "macos")]
    {
        return read_text_macos();
    }
    #[cfg(target_os = "windows")]
    {
        return read_text_windows();
    }
    #[cfg(target_os = "linux")]
    {
        return read_text_linux();
    }
    #[allow(unreachable_code)]
    None
}

/// A fresh temp path for the current process, so two running instances (or a
/// stale file from a crashed earlier run) never collide. We delete it after
/// reading; leaving a stray is harmless since it lives in the OS temp dir.
#[cfg(any(target_os = "macos", target_os = "windows"))]
fn scratch_path(ext: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("covexcode_clip_{}.{}", std::process::id(), ext))
}

#[cfg(target_os = "macos")]
fn read_macos() -> Option<ImageAttachment> {
    let path = scratch_path("png");
    let script = format!(
        "set pngData to the clipboard as «class PNGf»\n\
         set f to open for access POSIX file \"{}\" with write permission\n\
         set eof f to 0\n\
         write pngData to f\n\
         close access f",
        path.display()
    );
    let ok = Command::new("osascript")
        .arg("-e")
        .arg(script)
        .status()
        .ok()?
        .success();
    if !ok {
        return None;
    }
    read_scratch_and_drop(&path, "image/png")
}

#[cfg(target_os = "windows")]
fn read_windows() -> Option<ImageAttachment> {
    let path = scratch_path("png");
    let script = format!(
        "Add-Type -AssemblyName System.Windows.Forms,System.Drawing; \
         $img = [System.Windows.Forms.Clipboard]::GetImage(); \
         if ($img) {{ $img.Save('{}', [System.Drawing.Imaging.ImageFormat]::Png); exit 0 }} else {{ exit 1 }}",
        path.display()
    );
    let ok = Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", &script])
        .status()
        .ok()?
        .success();
    if !ok {
        return None;
    }
    read_scratch_and_drop(&path, "image/png")
}

#[cfg(target_os = "linux")]
fn read_linux() -> Option<ImageAttachment> {
    for (tool, args) in [
        ("wl-paste", vec!["--type", "image/png"]),
        (
            "xclip",
            vec!["-selection", "clipboard", "-t", "image/png", "-o"],
        ),
    ] {
        if let Ok(out) = Command::new(tool).args(args).output() {
            if out.status.success() && !out.stdout.is_empty() {
                return Some(ImageAttachment {
                    mime_type: "image/png".to_string(),
                    data_base64: crate::backend::base64_encode(&out.stdout),
                });
            }
        }
    }
    None
}

#[cfg(any(target_os = "macos", target_os = "windows"))]
fn read_scratch_and_drop(path: &std::path::Path, mime: &str) -> Option<ImageAttachment> {
    let bytes = std::fs::read(path).ok()?;
    let _ = std::fs::remove_file(path);
    if bytes.is_empty() {
        return None;
    }
    Some(ImageAttachment {
        mime_type: mime.to_string(),
        data_base64: crate::backend::base64_encode(&bytes),
    })
}

#[cfg(target_os = "macos")]
fn read_text_macos() -> Option<String> {
    let out = Command::new("pbpaste").output().ok()?;
    if out.status.success() && !out.stdout.is_empty() {
        String::from_utf8(out.stdout).ok().filter(|s| !s.is_empty())
    } else {
        None
    }
}

#[cfg(target_os = "windows")]
fn read_text_windows() -> Option<String> {
    let script = "Get-Clipboard";
    let out = Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", script])
        .output()
        .ok()?;
    if out.status.success() && !out.stdout.is_empty() {
        String::from_utf8(out.stdout)
            .ok()
            .filter(|s| !s.trim().is_empty())
    } else {
        None
    }
}

#[cfg(target_os = "linux")]
fn read_text_linux() -> Option<String> {
    for (tool, args) in [
        ("wl-paste", vec!["--type", "text/plain;charset=utf-8"]),
        ("wl-paste", vec!["--type", "text/plain"]),
        ("xclip", vec!["-selection", "clipboard", "-o"]),
    ] {
        if let Ok(out) = Command::new(tool).args(args).output() {
            if out.status.success() && !out.stdout.is_empty() {
                return String::from_utf8(out.stdout).ok().filter(|s| !s.is_empty());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rgba_to_png_produces_valid_png_header() {
        // 2x2 red image in RGBA
        let rgba = vec![
            255, 0, 0, 255, 255, 0, 0, 255, 255, 0, 0, 255, 255, 0, 0, 255,
        ];
        let png = rgba_to_png(2, 2, &rgba).expect("png encoding must succeed");
        assert!(!png.is_empty());
        assert_eq!(&png[0..8], b"\x89PNG\r\n\x1a\n");
    }

    #[test]
    fn rgba_to_png_fails_on_empty_or_zero_dimensions() {
        assert!(rgba_to_png(0, 0, &[]).is_none());
    }
}
