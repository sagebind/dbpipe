use std::io::{self, Write};

use sqlx::{Column, Row, TypeInfo, any::AnyRow};

use super::RowWriter;

pub struct JsonWriter<W> {
    writer: W,
}

impl<W: Write> JsonWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
        }
    }

    fn write_string(&mut self, string: &str) -> io::Result<()> {
        self.writer.write(b"\"")?;

        let mut char_buf = [0; 4];

        for c in string.chars() {
            match c {
                '"' | '\\' => {
                    self.writer.write(&[b'\\', c as u8])?;
                }
                c if c.is_control() => {
                    write!(&mut self.writer, "{}", c.escape_default())?;
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

            if column.type_info().is_null() {
                self.writer.write_all(b"null")?;
            } else {
                match column.type_info().name() {
                    "BOOLEAN" => write!(&mut self.writer, "{}", row.get::<bool, _>(i))?,
                    s if s.contains("INT") => write!(&mut self.writer, "{}", row.get::<i64, _>(i))?,
                    "FLOAT" => write!(&mut self.writer, "{}", row.get::<f32, _>(i))?,
                    "DOUBLE" => write!(&mut self.writer, "{}", row.get::<f64, _>(i))?,
                    _ => self.write_string(row.get::<String, _>(i).as_str())?,
                }
            }
        }

        self.writer.write_all(b"}\n")?;

        Ok(())
    }
}
