use std::io::Read;

use courier_ql::exec::{Executor, StepParsedOutput};
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
        for step in &plan.steps {
            println!("executing step {}...", step.name.unwrap_or("unnamed"));
            let output = executor.next().await?;
            println!("> {}", String::from_utf8_lossy(&output.raw_request));
            println!("< {}", String::from_utf8_lossy(&output.raw_response));
            match output.parsed {
                StepParsedOutput::HTTP(parsed) => {
                    println!("version: {}", parsed.version);
                    println!("status: {}", parsed.status);
                    println!("headers:");
                    for (k, v) in parsed.headers {
                        println!(
                            "    {}: {}",
                            k.map(|h| h.as_str().to_owned())
                                .unwrap_or("<missing>".to_string()),
                            v.to_str().unwrap()
                        );
                    }
                }
            }
        }
    }
    Ok(())
}
