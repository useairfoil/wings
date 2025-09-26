use std::borrow::Cow;
use std::{fmt, io};

use nu_ansi_term::{Color, Style};
use tracing::{Event, Level, Subscriber, field, span};

use tracing_subscriber::field::{VisitFmt, VisitOutput};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::{EnvFilter, Layer};
use tracing_subscriber::{prelude::*, registry::LookupSpan};

const OTEL_SDK_DISABLED: &str = "OTEL_SDK_DISABLED";

pub type BoxedLayer<S> = Box<dyn Layer<S> + Send + Sync>;

pub fn init_observability(
    _package_name: impl Into<Cow<'static, str>>,
    _package_version: impl Into<Cow<'static, str>>,
) {
    // The otel sdk doesn't follow the disabled env variable flag.
    // so we manually implement it to disable otel exports.
    // we diverge from the spec by defaulting to disabled.
    let sdk_disabled = std::env::var(OTEL_SDK_DISABLED)
        .map(|v| v == "true")
        .unwrap_or(true);

    if std::env::var("RUST_LOG").is_err() {
        unsafe {
            std::env::set_var("RUST_LOG", "info");
        }
    }

    let layers = vec![stdout()];

    if !sdk_disabled {
        todo!();
    }

    tracing_subscriber::registry().with(layers).init();
}

fn stdout<S>() -> BoxedLayer<S>
where
    S: Subscriber,
    for<'a> S: LookupSpan<'a>,
{
    let log_env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("INFO"));

    let json_fmt = std::env::var("RUST_LOG_FORMAT")
        .map(|val| val == "json")
        .unwrap_or(false);

    if json_fmt {
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_target(true)
            .json()
            .with_filter(log_env_filter)
            .boxed()
    } else {
        tracing_subscriber::fmt::layer()
            .with_ansi(true)
            .event_format(WingsFormat::default())
            .fmt_fields(WingsFormat::default())
            .with_filter(log_env_filter)
            .boxed()
    }
}

struct WingsFormat {
    time_format: time::format_description::OwnedFormatItem,
}

impl<S, N> FormatEvent<S, N> for WingsFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        let meta = event.metadata();

        write!(
            writer,
            "{}",
            FmtLevel::new(meta.level(), writer.has_ansi_escapes())
        )?;
        writer.write_char(' ')?;
        if self.format_time(&mut writer).is_err() {
            write!(writer, "[<unknown-timestamp>]")?;
        };
        writer.write_char(' ')?;

        ctx.format_fields(writer.by_ref(), event)?;

        writeln!(writer)
    }
}

impl<'w> FormatFields<'w> for WingsFormat {
    fn format_fields<R: __tracing_subscriber_field_RecordFields>(
        &self,
        writer: Writer<'w>,
        fields: R,
    ) -> fmt::Result {
        let mut v = WingsFormatVisitor::new(writer, true);
        fields.record(&mut v);
        v.finish()
    }

    fn add_fields(
        &self,
        current: &'w mut tracing_subscriber::fmt::FormattedFields<Self>,
        fields: &span::Record<'_>,
    ) -> fmt::Result {
        let empty = current.is_empty();
        let writer = current.as_writer();
        let mut v = WingsFormatVisitor::new(writer, empty);
        fields.record(&mut v);
        v.finish()
    }
}

struct WingsFormatVisitor<'a> {
    writer: Writer<'a>,
    is_empty: bool,
    style: Style,
    result: std::fmt::Result,
}

impl<'a> WingsFormatVisitor<'a> {
    fn new(writer: Writer<'a>, is_empty: bool) -> Self {
        Self {
            writer,
            is_empty,
            style: Style::new(),
            result: Ok(()),
        }
    }

    fn write_padded(&mut self, v: &impl fmt::Debug) {
        let padding = if self.is_empty {
            self.is_empty = false;
            ""
        } else {
            " "
        };

        self.result = write!(self.writer, "{}{:?}", padding, v);
    }
}

impl field::Visit for WingsFormatVisitor<'_> {
    fn record_str(&mut self, field: &field::Field, value: &str) {
        if self.result.is_err() {
            return;
        }

        if field.name() == "message" {
            self.record_debug(field, &format_args!("{:0<60}", value))
        } else {
            self.record_debug(field, &value)
        }
    }

    fn record_debug(&mut self, field: &field::Field, value: &dyn fmt::Debug) {
        if self.result.is_err() {
            return;
        }

        let value = format!("{:?}", value);
        match field.name() {
            "message" => {
                self.write_padded(&format_args!("{}{:<40}", self.style.prefix(), value));
            }
            name => {
                let color = if field.name() == "error" {
                    Color::Red
                } else {
                    Color::Blue
                };

                if self.writer.has_ansi_escapes() {
                    self.write_padded(&format_args!(
                        "{}{}={}",
                        self.style.prefix(),
                        name,
                        color.paint(value)
                    ));
                } else {
                    self.write_padded(&format_args!("{}{}={}", self.style.prefix(), name, value));
                }
            }
        }
    }
}

impl VisitOutput<std::fmt::Result> for WingsFormatVisitor<'_> {
    fn finish(mut self) -> std::fmt::Result {
        write!(&mut self.writer, "{}", self.style.suffix())?;
        self.result
    }
}

impl VisitFmt for WingsFormatVisitor<'_> {
    fn writer(&mut self) -> &mut dyn fmt::Write {
        &mut self.writer
    }
}

impl WingsFormat {
    pub fn format_time(&self, writer: &mut Writer<'_>) -> std::fmt::Result {
        let now = time::OffsetDateTime::from(std::time::SystemTime::now());
        let mut w = WriteAdaptor { fmt_writer: writer };
        now.format_into(&mut w, &self.time_format)
            .map_err(|_| std::fmt::Error)?;
        Ok(())
    }
}

struct FmtLevel<'a> {
    level: &'a Level,
    ansi: bool,
}

struct WriteAdaptor<'a> {
    fmt_writer: &'a mut dyn fmt::Write,
}

impl<'a> FmtLevel<'a> {
    fn new(level: &'a Level, ansi: bool) -> Self {
        Self { level, ansi }
    }
}

impl std::fmt::Display for FmtLevel<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.ansi {
            match *self.level {
                Level::TRACE => write!(f, "[{}]", Color::Purple.paint("TRACE")),
                Level::DEBUG => write!(f, "[{}]", Color::Blue.paint("DEBUG")),
                Level::INFO => write!(f, "[{}]", Color::Green.paint("INFO")),
                Level::WARN => write!(f, "[{}]", Color::Yellow.paint("WARN")),
                Level::ERROR => write!(f, "[{}]", Color::Red.paint("ERROR")),
            }
        } else {
            match *self.level {
                Level::TRACE => write!(f, "[TRACE]"),
                Level::DEBUG => write!(f, "[DEBUG]"),
                Level::INFO => write!(f, "[INFO]"),
                Level::WARN => write!(f, "[WARN]"),
                Level::ERROR => write!(f, "[ERROR]"),
            }
        }
    }
}

impl io::Write for WriteAdaptor<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let s =
            std::str::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        self.fmt_writer.write_str(s).map_err(io::Error::other)?;

        Ok(s.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Default for WingsFormat {
    fn default() -> Self {
        let time_format = time::format_description::parse_owned::<2>(
            r#"\[[month]-[day]|[hour]:[minute]:[second].[subsecond digits:3]\]"#,
        )
        .expect("failed to parse time format");

        Self { time_format }
    }
}
