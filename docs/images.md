# Images & Clipboard

Covexcode allows you to attach screenshots, copied image files, and local image paths directly into your conversation turns. Multi-modal models can inspect UI layouts, debug screenshot errors, analyze architecture diagrams, and review design assets alongside your codebase.

```
  ────────────────────────────────────────────────────────────────
  covexcode  ·  ↵ send  ·  Alt+V or /paste to attach image  ·  ^c exit
```

---

## Quick Reference

| Action | macOS | Windows (Windows Terminal) | Linux (X11 / Wayland) |
| :--- | :--- | :--- | :--- |
| **Paste Screenshot** | `Cmd+V` or `Ctrl+V` | **`Alt+V`** or `/paste` | `Ctrl+V` or `Alt+V` |
| **Paste Copied File** | `Cmd+V` (Finder) | **`Alt+V`** (File Explorer) | `Ctrl+V` (File Manager) |
| **Slash Command** | `/paste` | `/paste` | `/paste` |
| **Path Command** | `/image <path>` or `/img <path>` | `/image <path>` or `/img <path>` | `/image <path>` or `/img <path>` |
| **Drag & Drop** | Drag file into terminal | Drag file into terminal | Drag file into terminal |
| **Supported Formats** | PNG, JPEG, GIF, WebP, BMP | PNG, JPEG, GIF, WebP, BMP | PNG, JPEG, GIF, WebP, BMP |
| **Maximum File Size** | 8 MB | 8 MB | 8 MB |

---

## How to Attach Images

### 1. Screenshots from Clipboard
* **Windows**: Press `Win + Shift + S`, select your screen region, switch to Covexcode, and press **`Alt+V`** (or type `/paste` and press Enter).
* **macOS**: Press `Cmd + Ctrl + Shift + 4`, select a region, switch to Covexcode, and press **`Cmd+V`** (or `Ctrl+V`).
* **Linux**: Take a screenshot with your desktop screenshot utility (saving to clipboard), then press `Ctrl+V` or `Alt+V`.

An **`[Image #1] `** chip will appear in your prompt buffer, staging the image for your next message.

### 2. Copying Image Files from File Explorer / Finder
You do not need to open the image. Simply:
1. Select any image file in **Windows File Explorer** or **macOS Finder**.
2. Press `Ctrl+C` (or `Cmd+C` on Mac) to copy the file.
3. In Covexcode, press **`Alt+V`** (Windows) or **`Cmd+V`** (Mac).
4. Covexcode reads the native file handle from the clipboard and stages it immediately.

### 3. Drag and Drop
Drag an image file from your desktop or folder directly into the Covexcode terminal window. 
* Covexcode automatically detects the dropped path (including paths with spaces, quotes, or `file:///` prefixes) and converts it into an image chip (`[Image #1] `) without cluttering your prompt.

### 4. Direct File Paths (`/image`)
To attach an image on disk by path:
```text
/image assets/screenshot.png
/img "C:\Users\Me\Desktop\bug report.png"
```
Or type the path directly within your prompt:
```text
explain what is happening in ./diagram.png and how to fix it
```

---

## Why `Alt+V` on Windows? (Industry Architecture)

If you have used terminal-based AI tools like **Claude Code** or **OpenCode**, you may have noticed that image pasting in Windows Terminal requires `Alt+V` or `/paste` rather than `Ctrl+V`.

### The Windows Terminal Interception Problem
Terminal emulators were historically built for streaming plain text. Windows Terminal binds `Ctrl+V` in its default `settings.json` to its internal text-paste handler:
1. When you press `Ctrl+V`, Windows Terminal intercepts the keystroke **before** it reaches the console input buffer (`ReadConsoleInputW`).
2. It inspects the Windows system clipboard for plain text (`CF_UNICODETEXT`).
3. If the clipboard contains an image (e.g. from `Win + Shift + S`) and no text, Windows Terminal sees nothing to paste and **silently drops the keystroke**. The child application never receives a `KeyEvent`.

### The Solution
* **`Alt+V`**: Windows Terminal does not intercept `Alt` chords for pasting. `Alt+V` is forwarded directly as a console key event.
* **`/paste`**: A dedicated slash command that triggers clipboard reading regardless of your terminal configuration.
* **`Ctrl+V`**: Still supported and forwarded on terminals (like Windows Console Host or custom configurations) that do not swallow it.

---

## Under the Hood: Technical Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                        Terminal Input Triggers                         │
│         macOS: Cmd+V / /paste     │    Windows: Alt+V / /paste         │
│         Linux: Ctrl+V / /paste    │    Drag-and-Drop file paths        │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                         [handle_paste_key()]
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                   Non-Blocking Worker Thread (mpsc)                    │
│   • Main event loop renders "Reading clipboard…" instantly             │
│   • UI never freezes or drops frames                                  │
│   • Esc / submit cancels in-flight reads safely                        │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
         ┌──────────────────────────┴──────────────────────────┐
         ▼                                                     ▼
┌──────────────────────────────┐        ┌──────────────────────────────┐
│       macOS Backend          │        │       Windows Backend        │
│ • Native Cocoa NSPasteboard  │        │ • Win32 OpenClipboard       │
│ • Direct memory read (<5ms)  │        │ • CF_DIB / CF_DIBV5 bitmap   │
│ • Subprocess fallback:       │        │ • CF_HDROP File Explorer copy│
│   osascript (class PNGf)     │        │ • 5x retry loop on busy lock │
│ • Bracketed paste enabled    │        │ • Bracketed paste disabled   │
└──────────────────────────────┘        └──────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                   Image Optimization & Normalization                   │
│   • Large screenshots auto-resized to fit 1600x1600                    │
│   • Opaque images re-encoded as high-quality JPEG (10-15x reduction)   │
│   • RGBA PNG preserved for diagrams with alpha channels                │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                         Vision Model Pipeline                          │
│   • Staged chip [Image #1] added to prompt                             │
│   • Text-only model (e.g. deepseek-v4-pro) prompts:                   │
│     "Switch to deepseek-v4-flash? [y/n]"                              │
│   • Attachments are NEVER discarded on rejection or typing             │
└────────────────────────────────────────────────────────────────────────┘
```

### 1. Subprocess-Free Native Clipboard (<10ms)
Earlier CLI implementations called `powershell.exe` to access Windows Forms clipboard APIs. Initializing the .NET CLR took **1,500ms – 3,000ms**, causing visible terminal freezes.
* Covexcode uses native Win32 memory APIs via `arboard`:
  - **`CF_DIB` / `CF_DIBV5`**: Directly reads raw device-independent bitmap memory from screenshots.
  - **`CF_HDROP`**: Directly extracts file lists when images are copied in File Explorer.
  - Clipboard extraction executes in **under 10 milliseconds**.

### 2. Lock Contention & Retry Handling
On Windows, clipboard access can fail with `ERROR_ACCESS_DENIED` (`ClipboardOccupied`) if another process (such as Windows Clipboard History `Win + V` or anti-virus) holds the clipboard mutex.
* Covexcode implements a specialized retry loop:
  - If `ClipboardOccupied`: Retries up to 5 times with 15ms backoff.
  - If `ContentNotAvailable`: Returns immediately with zero delay (no lag when pasting with an empty clipboard).

### 3. Non-Blocking Threading & Debouncing
Clipboard reading never runs on the main crossterm UI thread.
* `handle_paste_key()` posts a `"Reading clipboard…"` system notice and spawns a background thread communicating over an `mpsc` channel.
* The main loop polls via non-blocking `try_recv()`.
* **Debounce**: Pressing `Alt+V` rapidly while a read is pending is ignored.
* **Cancellation**: Pressing `Esc` or typing and pressing `Enter` clears the listener so stale results never attach to subsequent prompts.

### 4. Bracketed Paste Isolation on Windows
Under Unix (macOS & Linux), terminal emulators wrap pasted text in escape codes (`\x1b[200~` and `\x1b[201~`).
* Crossterm on Windows processes Win32 `INPUT_RECORD`s rather than VT sequences. Enabling bracketed paste on Windows causes the terminal to leak raw `\x1b[200~` text into the prompt.
* Covexcode gates bracketed paste strictly behind `#[cfg(unix)]`, keeping Windows terminal input pristine.

### 5. Safe Drag-and-Drop Keystroke Gating
In Windows Terminal, dragging a file sends a rapid burst of raw character keystrokes.
* Path parsing only activates when a delimiter (`"`, `'`, or `' '`) is encountered.
* Normal typing (`a-z`, `0-9`) bypasses path checks entirely. This ensures:
  1. No filesystem `metadata()` syscalls are executed during normal typing, eliminating input lag.
  2. Typing a path like `notes.png.bak` will never convert prematurely into an image chip when `'g'` is typed.

### 6. Memory Limits & Pre-Allocation Guards
To prevent memory exhaustion from massive images or corrupted files:
* **Clipboard Bitmaps**: Dimensions capped at `8192 x 8192` and raw buffer sizes capped at `64 MB` before heap allocation.
* **File Attachments**: File size verified using `std::fs::metadata(path)` before reading bytes into memory (`MAX_IMAGE_BYTES = 8 MB`).
* **Image Optimization**: Screenshots larger than 1600px are downsampled with high-quality filtering, and opaque PNGs are converted to high-quality JPEG, reducing API payload sizes by 10–15x.

---

## Vision Model Compatibility

Multi-modal support depends on your active model:

| Provider | Model | Vision Capable? | Auto-Recommendation |
| :--- | :--- | :--- | :--- |
| **DeepSeek** | `deepseek-v4-flash` | **Yes** | Default vision model |
| **DeepSeek** | `deepseek-v4-pro` | Text-only | Prompts to switch to `deepseek-v4-flash` |
| **OpenAI** | `gpt-5.6-sol`, `gpt-4o` | **Yes** | Capable |
| **Custom / Anthropic**| Claude 3.5 / 3.7 | **Yes** | Capable |

### What happens if you attach an image with a text-only model?
If your current model does not support images (such as `deepseek-v4-pro`):
1. The image chip `[Image #1] ` remains staged.
2. Covexcode prompts:
   ```text
   Model deepseek-v4-pro does not support vision. Switch to deepseek-v4-flash? [y/n]
   ```
3. Pressing **`y`** automatically switches your model to `deepseek-v4-flash` and keeps the staged image.
4. Pressing **`n`** keeps your current model and preserves the attachment for manual review.
5. Typing any normal key immediately dismisses the prompt without swallowing your input.

---

## Frequently Asked Questions (FAQ)

#### Q: Why does `Ctrl+V` not work for images in Windows Terminal?
Windows Terminal handles `Ctrl+V` internally to paste text. When your clipboard only holds an image (e.g. from `Win + Shift + S`), Windows Terminal drops the keystroke completely. Use **`Alt+V`** or type **/paste**.

#### Q: Can I attach multiple images in one turn?
Yes. You can paste or drag multiple images into the same prompt. Each image receives a distinct chip (`[Image #1]`, `[Image #2]`).

#### Q: How do I remove a staged image?
Use Backspace to delete the `[Image #N]` chip from your prompt buffer, or press `Ctrl+U` to clear the line. Staged images are cleared when their chip is removed.

#### Q: Does Covexcode support images in headless / CI mode?
Yes. The headless API accepts base64-encoded image attachments directly via protocol message structures.
