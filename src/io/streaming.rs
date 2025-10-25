use std::io::{BufReader, BufWriter, Read, Seek, Write};

/// Convenience alias for the default buffered reader.
pub type BufferedReader<R> = BufReader<R>;
/// Convenience alias for the default buffered writer.
pub type BufferedWriter<W> = BufWriter<W>;

/// Wraps a reader so that it is suitable for FST parsing.
#[allow(dead_code)]
pub fn wrap_reader<R: Read + Seek>(reader: R) -> BufReader<R> {
    BufReader::new(reader)
}

/// Wraps a writer so that it is suitable for FST emission.
#[allow(dead_code)]
pub fn wrap_writer<W: Write + Seek>(writer: W) -> BufWriter<W> {
    BufWriter::new(writer)
}
