use std::io::Read;

use arboard::Clipboard;

pub fn run(save: bool) -> anyhow::Result<()> {
    let mut clipboard = Clipboard::new()?;
    if save {
        let mut buffer = String::new();
        std::io::stdin().read_to_string(&mut buffer)?;
        clipboard.set_text(buffer).unwrap();
    } else {
        print!("{}", clipboard.get_text().unwrap());
    }

    Ok(())
}
