use std::io::Read;

use arboard::Clipboard;

pub fn run(save: bool) -> anyhow::Result<()> {
    let mut clipboard = Clipboard::new()?;
    if save {
        let mut buffer = String::new();
        std::io::stdin().read_to_string(&mut buffer)?;
        clipboard.set_text(buffer).unwrap();
    } else {
        let text = clipboard.get_text()?;
        if text.is_empty() {
            return Err(anyhow::anyhow!("Clipboard is empty"));
        }
    }
    Ok(())
}
