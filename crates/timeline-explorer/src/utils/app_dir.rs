use dirs;
use std::path::PathBuf;

const APP_NAME: &str = "timeline-explorer";

/// Returns:
/// - macOS: ~/Library/Application Support/timeline-explorer
/// - Windows: %LOCALAPPDATA%\timeline-explorer
/// - Linux: ~/.local/share/timeline-explorer
pub fn get_local_app_path() -> Option<PathBuf> {
    dirs::data_local_dir().map(|path| path.join(APP_NAME))
}

/// Returns:
/// - macOS: ~/Library/Preferences
/// - Windows: %APPDATA% (e.g., C:\Users\{username}\AppData\Roaming)
/// - Linux: ~/.config
pub fn get_config_path() -> Option<PathBuf> {
    dirs::config_dir().map(|path| path.join(APP_NAME))
}

// Create the app directory if it doesn't exist
pub fn ensure_app_dir_exists() -> anyhow::Result<()> {
    if let Some(path) = get_local_app_path() {
        std::fs::create_dir_all(&path)?;
        Ok(())
    } else {
        Err(anyhow::anyhow!("Could not determine local app path"))
    }
}

// Create the config directory if it doesn't exist
pub fn ensure_config_dir_exists() -> anyhow::Result<()> {
    if let Some(path) = get_config_path() {
        std::fs::create_dir_all(&path)?;
        Ok(())
    } else {
        Err(anyhow::anyhow!("Could not determine config path"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_app_path() {
        let path = get_local_app_path();
        assert!(path.is_some());
        println!("Local app path: {:?}", path);
    }

    #[test]
    fn test_config_path() {
        let path = get_config_path();
        assert!(path.is_some());
        println!("Config path: {:?}", path);
    }
}
