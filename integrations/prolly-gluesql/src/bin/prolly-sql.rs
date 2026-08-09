use {
    clap::{Parser, Subcommand},
    prolly_gluesql::{Glue, Payload, ProllyStorageConfig, SqliteProllyStorage},
    std::{
        error::Error,
        fs,
        io::{self, Write},
        path::PathBuf,
    },
};

#[derive(Debug, Parser)]
#[command(
    name = "prolly-sql",
    version,
    about = "Versioned SQL powered by GlueSQL and prolly-map"
)]
struct Arguments {
    /// SQLite file used for durable Prolly nodes and branch roots.
    #[arg(short, long, default_value = "prolly.db")]
    database: PathBuf,

    /// Database branch to open.
    #[arg(short, long, default_value = "main")]
    branch: String,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Execute one SQL string and emit JSON payloads.
    Execute {
        /// SQL text. Multiple arguments are joined with spaces.
        #[arg(required = true)]
        sql: Vec<String>,
    },
    /// Execute all SQL in a UTF-8 file.
    File {
        /// SQL file to execute.
        path: PathBuf,
    },
    /// Start an interactive SQL shell.
    Shell,
    /// Create a branch from the selected branch's current state.
    Branch {
        /// New branch name.
        name: String,
    },
    /// Print the selected branch and immutable root CID.
    Head,
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn Error>> {
    let arguments = Arguments::parse();
    let config = ProllyStorageConfig {
        branch: arguments.branch,
        ..ProllyStorageConfig::default()
    };
    let storage = SqliteProllyStorage::open_sqlite_with_config(&arguments.database, config)?;
    let mut glue = Glue::new(storage);

    match arguments.command.unwrap_or(Command::Shell) {
        Command::Execute { sql } => execute(&mut glue, &sql.join(" ")).await?,
        Command::File { path } => execute(&mut glue, &fs::read_to_string(path)?).await?,
        Command::Shell => shell(&mut glue).await?,
        Command::Branch { name } => {
            let version = glue.storage.create_branch(&name)?;
            println!(
                "created branch {:?} at {}",
                version.branch,
                display_root(&version)
            );
        }
        Command::Head => match glue.storage.head()? {
            Some(version) => println!("{} {}", version.branch, display_root(&version)),
            None => println!("{} <unborn>", glue.storage.branch()),
        },
    }
    Ok(())
}

async fn execute(
    glue: &mut Glue<SqliteProllyStorage>,
    sql: &str,
) -> std::result::Result<(), Box<dyn Error>> {
    let payloads = glue.execute(sql).await?;
    print_payloads(&payloads)?;
    Ok(())
}

fn print_payloads(payloads: &[Payload]) -> std::result::Result<(), serde_json::Error> {
    for payload in payloads {
        println!("{}", serde_json::to_string_pretty(payload)?);
    }
    Ok(())
}

async fn shell(glue: &mut Glue<SqliteProllyStorage>) -> std::result::Result<(), Box<dyn Error>> {
    println!(
        "prolly-sql on branch {:?}; enter .help for shell commands",
        glue.storage.branch()
    );
    let stdin = io::stdin();
    let mut statement = String::new();
    loop {
        print!(
            "{}",
            if statement.is_empty() {
                "prolly> "
            } else {
                "    -> "
            }
        );
        io::stdout().flush()?;
        let mut line = String::new();
        if stdin.read_line(&mut line)? == 0 {
            break;
        }
        let trimmed = line.trim();
        if statement.is_empty() && trimmed.starts_with('.') {
            match trimmed {
                ".quit" | ".exit" => break,
                ".help" => {
                    println!(".head              show the selected immutable root");
                    println!(".branch NAME       switch to an existing branch");
                    println!(".quit              exit the shell");
                }
                ".head" => match glue.storage.head()? {
                    Some(version) => {
                        println!("{} {}", version.branch, display_root(&version));
                    }
                    None => println!("{} <unborn>", glue.storage.branch()),
                },
                command if command.starts_with(".branch ") => {
                    let branch = command.trim_start_matches(".branch ").trim();
                    match glue.storage.checkout_branch(branch) {
                        Ok(()) => println!("switched to branch {branch:?}"),
                        Err(error) => eprintln!("error: {error}"),
                    }
                }
                _ => eprintln!("unknown shell command; enter .help"),
            }
            continue;
        }

        statement.push_str(&line);
        if !trimmed.ends_with(';') {
            continue;
        }
        match glue.execute(&statement).await {
            Ok(payloads) => print_payloads(&payloads)?,
            Err(error) => eprintln!("error: {error}"),
        }
        statement.clear();
    }
    Ok(())
}

fn display_root(version: &prolly_gluesql::DatabaseVersion) -> String {
    version.root().map_or_else(
        || "<empty>".to_owned(),
        |cid| {
            cid.as_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect()
        },
    )
}
