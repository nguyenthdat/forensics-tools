use eframe::{
    NativeOptions,
    egui::{ViewportBuilder, Visuals},
};
// use std::{env, path::PathBuf};

mod app;
mod config;
mod util;

const APP_TITLE: &str = "Waka Forensics Suite";

fn main() -> eframe::Result<()> {
    // let args: Vec<String> = env::args().collect();
    // let file = args.get(1).map(PathBuf::from);

    let native_options = NativeOptions {
        viewport: ViewportBuilder::default()
            .with_title(APP_TITLE)
            .with_min_inner_size([800.0, 600.0])
            .with_taskbar(true)
            .with_inner_size([1200.0, 800.0])
            .with_icon(eframe::icon_data::from_png_bytes(&[]).unwrap_or_default()),
        ..Default::default()
    };

    eframe::run_native(
        APP_TITLE,
        native_options,
        Box::new(|cc| {
            // Configure egui style here if needed
            cc.egui_ctx.set_visuals(Visuals::dark());

            Ok(Box::new(app::WakaApp::new()))
        }),
    )
}
