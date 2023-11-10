use std::io::Read;

use courier_ql::exec::Executor;
use courier_ql::{Plan, StepOutput};

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
        for (name, _) in plan.steps.iter() {
            println!("executing step {}...", name);
            let output = executor.next().await?;
            match output {
                StepOutput::HTTP(http)
                | StepOutput::HTTP11(http)
                | StepOutput::HTTP2(http)
                | StepOutput::HTTP3(http) => {
                    println!(
                        "> {}",
                        String::from_utf8_lossy(&http.raw_request).replace("\n", "\n> ")
                    );
                    println!(
                        "< {}",
                        String::from_utf8_lossy(&http.raw_response).replace("\n", "\n< ")
                    );
                    println!("protocol: {:?}", http.response.protocol);
                    println!("status: {}", http.response.status_code);
                    println!("headers:");
                    for (k, v) in http.headers {
                        println!("    {}: {}", k, v);
                    }
                }
                StepOutput::TCP(tcp) => {}
            }
        }
    }
    Ok(())
}
