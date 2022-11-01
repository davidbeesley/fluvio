use clap::Parser;
use color_eyre::eyre::Result;
use fluvio_cli::{Root, HelpOpt};
use fluvio_future::task::run_block_on;

fn main() -> Result<()> {
    fluvio_future::subscriber::init_tracer(None);
    color_eyre::config::HookBuilder::blank()
        .display_env_section(false)
        .install()?;
    print_help_hack()?;
    let root: Root = Root::parse();

    // If the CLI comes back with an error, attempt to handle it
    if let Err(e) = run_block_on(root.process()) {
        let user_error = e.get_user_error()?;
        eprintln!("{}", user_error);
        std::process::exit(1);
    }

    Ok(())
}

fn print_help_hack() -> Result<()> {
    let mut args = std::env::args();
    if args.len() < 2 {
        HelpOpt {}.process()?;
        std::process::exit(0);
    } else if let Some(first_arg) = args.nth(1) {
        // We pick help up here as a courtesy
        if vec!["-h", "--help", "help"].contains(&first_arg.as_str()) {
            HelpOpt {}.process()?;
            std::process::exit(0);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use fluvio_cli::Root;

    #[test]
    fn test_correct_command_parsing_help() {
        assert!(parse("fluvio").is_err());
        assert!(parse("fluvio -h").is_err());
        assert!(parse("fluvio --help").is_err());
    }

    #[test]
    fn test_correct_command_parsing_consume() {
        assert!(parse("fluvio consume -H hello").is_ok());
        assert!(parse("fluvio consume -T hello").is_ok());
        assert!(parse("fluvio consume -H -n 10 hello").is_ok());
        assert!(parse("fluvio consume -T -n 10 hello").is_ok());
        assert!(parse("fluvio consume --start -n 0 hello").is_ok());
        assert!(parse("fluvio consume --start hello").is_ok());
        assert!(parse("fluvio consume hello --start --end 5").is_ok());
        assert!(parse("fluvio consume --start --end 5 -n 0 hello").is_ok());

        assert!(parse("fluvio consume").is_err());
        assert!(parse("fluvio consume -H 0 hello").is_err());
        assert!(parse("fluvio consume -T 0 hello").is_err());
        assert!(parse("fluvio consume -H -n -10 hello").is_err());
        assert!(parse("fluvio consume -H -n hello").is_err());
        assert!(parse("fluvio consume -n 10 hello").is_err());
        assert!(parse("fluvio consume -H -T -n 10 hello").is_err());
        assert!(parse("fluvio consume -n hello").is_err());
        assert!(parse("fluvio consume --start 5 hello").is_err());
        assert!(parse("fluvio consume --end hello").is_err());
    }

    #[test]
    fn test_supply_negative_end_offset() {
        assert!(parse("fluvio consume --start --end 5 -n 0 hello").is_ok());
        assert!(parse("fluvio consume --end 5 hello").is_ok());

        assert!(parse("fluvio consume --end -5 hello").is_err());
        assert!(parse("fluvio consume --start --end -5 -n 0 hello").is_err());
    }

    fn parse(command: &str) -> Result<Root, clap::error::Error> {
        Root::try_parse_from(command.split_whitespace())
    }
}
