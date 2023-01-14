use std::time;

use tui::{backend::Backend, Terminal};

use app::App;
use input::{Events, InputEvent};
use panel::Signal;

mod app;
mod input;
mod panel;

pub fn run<B: Backend + 'static>(term: &mut Terminal<B>) -> Result<(), Box<dyn std::error::Error>> {
    // Setup tokio runtime.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let _guard = runtime.enter();

    // User event handler
    let tick_rate = time::Duration::from_millis(200);
    let mut events = Events::new(tick_rate);

    let mut app = App::new();
    term.draw(|f| app.draw(f))?;

    loop {
        // Handle inputs
        let signals = match events.next() {
            InputEvent::Input(e) => app.event(e),
            InputEvent::Tick => app.tick(),
        };
        for signal in signals {
            match signal {
                Signal::Exit => return Ok(()),
                Signal::NavStackPush(_) => unreachable!(),
                Signal::NavStackPop => unreachable!(),
            }
        }
        // Render
        term.draw(|f| app.draw(f))?;
    }
}
