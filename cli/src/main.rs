use std::io::Read;

use courier_ql::Plan;

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
        for step in plan.steps {
            step.exec().await?;
        }
    }
    Ok(())
}
