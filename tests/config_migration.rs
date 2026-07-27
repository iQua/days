use std::fs;

use tempfile::NamedTempFile;

#[test]
fn removed_scheduler_batch_key_has_a_migration_diagnostic() {
    let config = NamedTempFile::new().unwrap();
    let legacy_key = ["run", "batch", "size"].join("_");
    fs::write(
        config.path(),
        format!(
            "seed = 1\n\
             [switch]\n\
             {legacy_key} = 2\n"
        ),
    )
    .unwrap();

    let error = days::run_simulation_from_config(config.path().to_str().unwrap()).unwrap_err();
    assert_eq!(
        error,
        format!(
            "Configuration key `switch.{legacy_key}` was removed; \
             schedulers now select one packet per service start."
        )
    );
}
