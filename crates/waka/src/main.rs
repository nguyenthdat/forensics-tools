use eframe::NativeOptions;
// use std::{env, path::PathBuf};

mod app;
mod config;
mod util;

fn main() -> eframe::Result<()> {
    // let args: Vec<String> = env::args().collect();
    // let file = args.get(1).map(PathBuf::from);

    let native_options = NativeOptions::default();
    eframe::run_native(
        "Waka - Forensic Analysis Tool",
        native_options,
        Box::new(move |_cc| Ok(Box::new(app::WakaApp::new()))),
    )
}
