use chrono::{SecondsFormat, Utc};
use env_logger::fmt::Formatter;
use log::{kv, Level, Record};
use serde::Serialize;
use std::io::Write;

pub trait SerdeLogging {
    fn as_json_str(&self) -> String;
}

impl<T: Serialize> SerdeLogging for T {
    #[cfg(debug_assertions)]
    fn as_json_str(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| "{}".to_owned())
    }

    #[cfg(not(debug_assertions))]
    fn as_json_str(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_owned())
    }
}

fn level_to_str(level: Level) -> &'static str {
    match level {
        Level::Error => "Error",
        Level::Debug => "Debug",
        Level::Info => "Info",
        Level::Warn => "Warn",
        Level::Trace => "Trace",
    }
}

fn log_params<'a>(record: &'a Record) -> (&'static str, String, &'a str, &'a str, u32) {
    let severity = level_to_str(record.metadata().level());
    let date = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let module_path = record.module_path().unwrap_or_default();
    let file = record.file().unwrap_or_default();
    let line = record.line().unwrap_or_default();
    (severity, date, module_path, file, line)
}

#[cfg(any(test, debug_assertions))]
fn write_debug(f: &mut Formatter, record: &log::Record) -> std::io::Result<()> {
    use env_logger::fmt::Color;

    struct LogVisitor<'a, W: Write> {
        writer: &'a mut W,
    }
    impl<'kvs, 'a, W: Write> kv::Visitor<'kvs> for LogVisitor<'a, W> {
        fn visit_pair(&mut self, key: kv::Key<'kvs>, val: kv::Value<'kvs>) -> Result<(), kv::Error> {
            write!(self.writer, "\n{} {}", key, val).unwrap();
            Ok(())
        }
    }

    // let project_dir = env!("CARGO_MANIFEST_DIR");
    let (severity, date, module_path, file, line) = log_params(record);
    // #[rustfmt::skip]

    let mut level_style = f.default_level_style(record.level());
    level_style.set_bold(true);

    let mut general_style = f.style();
    general_style.set_color(Color::Rgb(33, 33, 33));

    let mut module_style = f.style();
    module_style.set_color(Color::Yellow);

    write!(
        f,
        "{}  {:5}  {} ({}:{})  {}",
        general_style.value(date),
        level_style.value(severity),
        module_style.value(module_path),
        general_style.value(file),
        general_style.value(line),
        record.args()
    )?;
    // key / value pairs
    let mut visitor = LogVisitor { writer: f };
    record.key_values().visit(&mut visitor).unwrap();
    writeln!(f)
}

#[cfg(debug_assertions)]
pub fn init() {
    env_logger::builder().format(write_debug).init();
}

#[cfg(any(test, not(debug_assertions)))]
fn write_json<F: Write>(f: &mut F, record: &log::Record) -> std::io::Result<()> {
    struct LogVisitor<'a, W: Write> {
        writer: &'a mut W,
    }
    impl<'kvs, 'a, W: Write> kv::Visitor<'kvs> for LogVisitor<'a, W> {
        fn visit_pair(&mut self, key: kv::Key<'kvs>, val: kv::Value<'kvs>) -> Result<(), kv::Error> {
            let vs = val.to_borrowed_str().unwrap_or_default();
            if serde_json::from_str::<serde_json::Value>(vs).is_ok() {
                write!(self.writer, ",\"{}\":{}", key, val).unwrap();
            } else {
                write!(self.writer, ",\"{}\":\"{}\"", key, val).unwrap();
            }
            Ok(())
        }
    }

    let (severity, date, module_path, file, line) = log_params(record);
    #[rustfmt::skip]
  write!(f, r#"{{"date":"{}","severity":"{}","message":"{}","className":"{}","file":"{}","line":{}"#, date, severity, record.args(), module_path, file, line)?;
    // key / value pairs
    let mut visitor = LogVisitor { writer: f };
    record.key_values().visit(&mut visitor).unwrap();
    writeln!(f, "}}")
}

#[cfg(not(debug_assertions))]
pub fn init() {
    env_logger::builder().format(write_json).init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // #[test]
    // fn test_write_debug() {
    //     let kvs = vec![("key1", "val1"), ("key2", "val2")];
    //     let record = log::Record::builder()
    //         .args(format_args!("hello"))
    //         .level(log::Level::Info)
    //         .module_path(Some("foo::bar"))
    //         .file(Some("src/file1.rs"))
    //         .line(Some(12))
    //         .key_values(&kvs)
    //         .build();
    //     let mut buf = Vec::new();
    //     write_debug(&mut buf, &record).unwrap();
    //     let output = std::str::from_utf8(&buf);
    //     assert!(output.is_ok())
    // }

    #[test]
    fn test_write_json() {
        let kvs = vec![("key1", "val1"), ("key2", "{\"foo\":12}")];
        let record = log::Record::builder()
            .args(format_args!("hello"))
            .level(log::Level::Info)
            .module_path(Some("foo::bar"))
            .file(Some("src/file1.rs"))
            .line(Some(12))
            .key_values(&kvs)
            .build();
        let mut buf = Vec::new();
        write_json(&mut buf, &record).unwrap();
        let output = std::str::from_utf8(&buf).unwrap();
        let json: serde_json::Value = serde_json::from_str(output).unwrap();
        assert_eq!(json["severity"], "Info");
        assert_eq!(json["message"], "hello");
        assert_eq!(json["className"], "foo::bar");
        assert_eq!(json["file"], "src/file1.rs");
        assert_eq!(json["line"], json!(12));
        assert_eq!(json["key1"], "val1");
        assert_eq!(json["key2"], json!({"foo":12}));
    }
}
