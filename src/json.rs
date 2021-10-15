use std::io::{self, Write};

use chrono::NaiveDateTime;
use sqlx::{any::AnyRow, Column, Row, TypeInfo, ValueRef};

use super::RowWriter;

pub struct JsonWriter<W> {
    writer: W,
}

impl<W: Write> JsonWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    fn write_string(&mut self, string: &str) -> io::Result<()> {
        self.writer.write(b"\"")?;

        let mut char_buf = [0; 4];

        for c in string.chars() {
            match c {
                '"' | '\\' => {
                    self.writer.write(&[b'\\', c as u8])?;
                }
                '\u{8}' => {
                    self.writer.write(b"\\b")?;
                }
                '\u{C}' => {
                    self.writer.write(b"\\f")?;
                }
                '\n' => {
                    self.writer.write(b"\\n")?;
                }
                '\r' => {
                    self.writer.write(b"\\r")?;
                }
                '\t' => {
                    self.writer.write(b"\\t")?;
                }
                c if c.is_control() => {
                    write!(&mut self.writer, "\\u{:04x}", c as u32)?;
                }
                c => {
                    self.writer.write(c.encode_utf8(&mut char_buf).as_bytes())?;
                }
            }
        }

        self.writer.write(b"\"")?;

        Ok(())
    }
}

impl<W: Write> RowWriter for JsonWriter<W> {
    fn write(&mut self, row: &AnyRow) -> io::Result<()> {
        self.writer.write(b"{")?;

        for (i, column) in row.columns().iter().enumerate() {
            if i > 0 {
                self.writer.write(b",")?;
            }

            self.write_string(column.name())?;
            self.writer.write(b":")?;

            if row.try_get_raw(i).unwrap().is_null() {
                self.writer.write_all(b"null")?;
            } else {
                match column.type_info().name() {
                    "BOOLEAN" => write!(&mut self.writer, "{}", row.get::<bool, _>(i))?,
                    s if s.contains("INT") => write!(&mut self.writer, "{}", row.get::<i64, _>(i))?,
                    "FLOAT" => write!(&mut self.writer, "{}", row.get::<f32, _>(i))?,
                    "DOUBLE" => write!(&mut self.writer, "{}", row.get::<f64, _>(i))?,
                    "CHAR" | "VARCHAR" | "TEXT" | "LONGTEXT" => {
                        self.write_string(row.get::<String, _>(i).as_str())?
                    }
                    "DATETIME" => {
                        self.write_string(row.get::<NaiveDateTime, _>(i).to_string().as_str())?
                    }
                    _ => self.writer.write_all(b"null")?,
                }
            }
        }

        self.writer.write_all(b"}\n")?;

        Ok(())
    }
}
