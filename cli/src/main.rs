use std::borrow::Cow;
use std::io::Read;

use courier_ql::exec::{Executor, StepOutput};
use courier_ql::{Plan, StepBody};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
async fn main() -> Result<()> {
    let mut buffer = Vec::new();
    let stdin = std::io::stdin();
    let mut handle = stdin.lock();

    handle.read_to_end(&mut buffer)?;

    let text = String::from_utf8(buffer)?;
    {
        let plan = Plan::parse(&text)?;
        let mut executor = Executor::new(&plan);
        for (i, step) in plan.steps.iter().enumerate() {
            let i_str = i.to_string();
            println!("executing step {}...", step.name.unwrap_or(&i_str));
            let output = executor.next().await?;
            match output {
                StepOutput::HTTP(http) => {
                    println!(
                        "> {}",
                        String::from_utf8_lossy(&http.raw_request).replace("\n", "\n> ")
                    );
                    println!(
                        "< {}",
                        String::from_utf8_lossy(&http.raw_response).replace("\n", "\n< ")
                    );
                    println!("version: {}", http.version);
                    println!("status: {}", http.status);
                    println!("headers:");
                    for (k, v) in http.headers {
                        println!(
                            "    {}: {}",
                            k.map(|h| h.as_str().to_owned())
                                .unwrap_or("<missing>".to_string()),
                            v.to_str().unwrap()
                        );
                    }
                }
                StepOutput::TCP(tcp) => {}
            }
        }
    }
    Ok(())
}
