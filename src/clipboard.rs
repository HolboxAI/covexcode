//! Read an image out of the OS clipboard, cross-platform.
//!
//! Terminal paste events (`Event::Paste`) deliver *text* only — the terminal
//! emulator never ships the bytes of an image on paste. To get an image the
//! user has copied (or a screenshot they took) we query the OS clipboard:
//!
//! * **Windows** — Native Win32 API via `arboard` (DIB/DIBV5 bitmap formats) and
//!   `file_list()` (`CF_HDROP` for files copied in Windows File Explorer).
//!   Retries on `ClipboardOccupied` and runs asynchronously in a worker thread.
//! * **macOS** — Native `arboard` with `osascript` fallback writing PNG to temp file.
//! * **Linux** — Native `arboard` with `wl-paste` (Wayland) and `xclip` (X11) fallbacks.
//!
//! Clipboard reads are executed on a dedicated background thread communicating
//! via an `mpsc` channel, keeping the terminal UI and event loop fully responsive.

use crate::llm::ImageAttachment;
use arboard::Clipboard;
#[cfg(any(target_os = "macos", target_os = "linux"))]
use std::process::Command;

/// Encode RGBA bytes into PNG bytes in memory.
pub fn rgba_to_png(width: u32, height: u32, rgba: &[u8]) -> Option<Vec<u8>> {
    if width == 0 || height == 0 || rgba.is_empty() || width > 8192 || height > 8192 || rgba.len() > 64 * 1024 * 1024 {
        return None;
    }
    let mut out = Vec::new();
    let mut encoder = png::Encoder::new(&mut out, width, height);
    encoder.set_color(png::ColorType::Rgba);
    encoder.set_depth(png::BitDepth::Eight);
    let mut writer = encoder.write_header().ok()?;
    writer.write_image_data(rgba).ok()?;
    drop(writer);
    Some(out)
}

/// Canonical MIME for a preferred type, so a caller never declares PNG while
/// handing the provider WebP/GIF bytes (or vice versa).
fn canonical_image_mime(preferred_mime: &str) -> &'static str {
    let mime = preferred_mime.to_ascii_lowercase();
    if mime.contains("jpeg") || mime.contains("jpg") {
        "image/jpeg"
    } else if mime.contains("gif") {
        "image/gif"
    } else if mime.contains("webp") {
        "image/webp"
    } else if mime.contains("bmp") {
        "image/bmp"
    } else if mime.contains("tiff") {
        "image/tiff"
    } else {
        "image/png"
    }
}

/// Optimize an image before staging/sending:
/// 1. If width or height exceeds 1600px, resize to fit 1600x1600 preserving aspect ratio.
/// 2. If the image is opaque, compress as high-quality JPEG (quality 85) to reduce payload size by 10-15x.
pub fn optimize_image(bytes: &[u8], preferred_mime: &str) -> (Vec<u8>, &'static str) {
    let original_mime = canonical_image_mime(preferred_mime);
    let Ok(mut img) = image::load_from_memory(bytes) else {
        return (bytes.to_vec(), original_mime);
    };

    let (w, h) = (img.width(), img.height());
    let resized = w > 1600 || h > 1600;
    if resized {
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
            if !res.is_empty() && res.len() < bytes.len() {
                return (res, "image/jpeg");
            }
        }
    }

    // If the pixels changed (resize) or the source was not already PNG/JPEG,
    // normalize to PNG so the declared MIME always matches the bytes. Keeping
    // the original bytes while claiming image/png made WebP/GIF attachments
    // fail at providers that trust the declared type.
    if resized || !matches!(original_mime, "image/png" | "image/jpeg") {
        let mut out = Vec::new();
        if img
            .write_to(
                &mut std::io::Cursor::new(&mut out),
                image::ImageFormat::Png,
            )
            .is_ok()
            && !out.is_empty()
        {
            return (out, "image/png");
        }
    }

    (bytes.to_vec(), original_mime)
}

/// Helper to retry clipboard operations on busy/locked error.
pub fn retry_clipboard_op<T, F>(mut op: F) -> Result<T, arboard::Error>
where
    F: FnMut() -> Result<T, arboard::Error>,
{
    const MAX_RETRIES: usize = 5;
    const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(15);

    let mut last_err = arboard::Error::ContentNotAvailable;
    for attempt in 0..MAX_RETRIES {
        match op() {
            Ok(val) => return Ok(val),
            Err(arboard::Error::ClipboardOccupied) => {
                last_err = arboard::Error::ClipboardOccupied;
                if attempt + 1 < MAX_RETRIES {
                    std::thread::sleep(RETRY_DELAY);
                }
            }
            Err(other) => return Err(other),
        }
    }
    Err(last_err)
}

/// Reads image from the clipboard with up to 5 retries on clipboard-busy/locked errors.
pub fn read_clipboard_image_with_retry() -> Result<arboard::ImageData<'static>, arboard::Error> {
    retry_clipboard_op(|| {
        let mut cb = Clipboard::new()?;
        let img = cb.get_image()?;
        if img.width == 0
            || img.height == 0
            || img.width > 8192
            || img.height > 8192
            || img.bytes.len() > 64 * 1024 * 1024
        {
            return Err(arboard::Error::ConversionFailure);
        }
        Ok(arboard::ImageData {
            width: img.width,
            height: img.height,
            bytes: std::borrow::Cow::Owned(img.bytes.into_owned()),
        })
    })
}

/// Reads copied image files from the clipboard (e.g. copied from Windows File Explorer).
pub fn read_clipboard_file_list() -> Option<ImageAttachment> {
    let mut cb = Clipboard::new().ok()?;
    let files = cb.get().file_list().ok()?;
    for path in files {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|s| s.to_ascii_lowercase())
            .unwrap_or_default();
        if matches!(ext.as_str(), "png" | "jpg" | "jpeg" | "gif" | "webp" | "bmp") {
            if let Ok(meta) = std::fs::metadata(&path) {
                if meta.len() == 0 || meta.len() > 8 * 1024 * 1024 {
                    continue;
                }
            } else {
                continue;
            }
            if let Ok(bytes) = std::fs::read(&path) {
                if !bytes.is_empty() {
                    let preferred_mime = match ext.as_str() {
                        "jpg" | "jpeg" => "image/jpeg",
                        "png" => "image/png",
                        "gif" => "image/gif",
                        "webp" => "image/webp",
                        _ => "image/png",
                    };
                    let (opt_bytes, mime) = optimize_image(&bytes, preferred_mime);
                    return Some(ImageAttachment {
                        mime_type: mime.to_string(),
                        data_base64: crate::backend::base64_encode(&opt_bytes),
                    });
                }
            }
        }
    }
    None
}

/// The image currently in the clipboard, base64-encoded as PNG or JPEG, or `None` if
/// the clipboard holds no image or the platform's clipboard tool is missing.
pub fn read_image() -> Option<ImageAttachment> {
    // 1. Try native in-memory clipboard with retry (instant, lag-free via arboard)
    if let Ok(img) = read_clipboard_image_with_retry() {
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

    // 2. Check for copied image files (e.g. from File Explorer)
    if let Some(attachment) = read_clipboard_file_list() {
        return Some(attachment);
    }

    // 3. Platform subprocess fallback (macOS/Linux)
    #[cfg(target_os = "macos")]
    {
        return read_macos();
    }
    #[cfg(target_os = "linux")]
    {
        return read_linux();
    }
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
#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
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

    #[test]
    fn canonical_mime_never_mislabels_a_webp_or_gif_as_png() {
        assert_eq!(canonical_image_mime("image/webp"), "image/webp");
        assert_eq!(canonical_image_mime("image/gif"), "image/gif");
        assert_eq!(canonical_image_mime("image/jpeg"), "image/jpeg");
        assert_eq!(canonical_image_mime("image/jpg"), "image/jpeg");
        assert_eq!(canonical_image_mime("image/tiff"), "image/tiff");
        assert_eq!(canonical_image_mime("image/png"), "image/png");
    }

    #[test]
    fn retry_clipboard_op_succeeds_after_transient_busy() {
        let mut attempts = 0;
        let res = retry_clipboard_op(|| {
            attempts += 1;
            if attempts <= 2 {
                Err(arboard::Error::ClipboardOccupied)
            } else {
                Ok("success")
            }
        });
        assert_eq!(res.unwrap(), "success");
        assert_eq!(attempts, 3);
    }

    #[test]
    fn retry_clipboard_op_fails_after_5_busy_attempts_without_hanging() {
        let mut attempts = 0;
        let res: Result<(), _> = retry_clipboard_op(|| {
            attempts += 1;
            Err(arboard::Error::ClipboardOccupied)
        });
        assert!(matches!(res, Err(arboard::Error::ClipboardOccupied)));
        assert_eq!(attempts, 5);
    }

    #[test]
    fn retry_clipboard_op_returns_immediately_on_content_not_available() {
        let mut attempts = 0;
        let res: Result<(), _> = retry_clipboard_op(|| {
            attempts += 1;
            Err(arboard::Error::ContentNotAvailable)
        });
        assert!(matches!(res, Err(arboard::Error::ContentNotAvailable)));
        assert_eq!(attempts, 1);
    }
}
