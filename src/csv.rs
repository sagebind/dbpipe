use std::io::{self, Write};

use sqlx::{Column, Row, TypeInfo, any::AnyRow};

use super::RowWriter;

pub struct CsvWriter<W> {
    writer: W,
    header_written: bool,
    columns_written: usize,
}

impl<W: Write> CsvWriter<W> {
    pub fn new(writer: W, header: bool) -> Self {
        Self {
            writer,
            header_written: !header,
            columns_written: 0,
        }
    }

    fn write_cell(&mut self, data: &[u8]) -> io::Result<()> {
        if self.columns_written > 0 {
            self.writer.write_all(b",")?;
        }

        let needs_quoted = data.iter().any(|&byte| byte == b'"' || byte == b',' || byte == b'\n');

        if needs_quoted {
            self.writer.write_all(b"\"")?;

            for &byte in data {
                if byte == b'"' {
                    self.writer.write_all(&[b'"'])?;
                }

                self.writer.write_all(&[byte])?;
            }

            self.writer.write_all(b"\"")?;
        } else {
            self.writer.write_all(data)?;
        }

        self.columns_written += 1;

        Ok(())
    }

    fn finish_row(&mut self) -> io::Result<()> {
        self.writer.write_all(b"\n")?;
        self.columns_written = 0;

        Ok(())
    }
}

impl<W: Write> RowWriter for CsvWriter<W> {
    fn write(&mut self, row: &AnyRow) -> io::Result<()> {
        if !self.header_written {
            for column in row.columns() {
                self.write_cell(column.name().as_bytes())?;
            }

            self.finish_row()?;

            self.header_written = true;
        }

        for (i, _) in row.columns().iter().enumerate() {
            if let Some(value) = get_str(&row, i) {
                self.write_cell(value.as_bytes())?;
            }
        }

        self.finish_row()?;

        Ok(())
    }
}

fn get_str(row: &AnyRow, index: usize) -> Option<String> {
    let type_info = row.column(index).type_info();

    if type_info.is_null() {
        None
    } else {
        Some(match row.column(index).type_info().name() {
            "BOOLEAN" => row.get::<bool, _>(index).to_string(),
            s if s.contains("INT") => row.get::<i64, _>(index).to_string(),
            "FLOAT" => row.get::<f32, _>(index).to_string(),
            "DOUBLE" => row.get::<f64, _>(index).to_string(),
            _ => row.get(index),
        })
    }
}
