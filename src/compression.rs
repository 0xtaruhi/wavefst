use libdeflater::{CompressionLvl, Compressor, Decompressor};

use crate::error::{Error, Result};

pub(crate) fn compressor(level: u32) -> Compressor {
    let level = i32::try_from(level.min(9)).expect("compression level fits in i32");
    let level = CompressionLvl::new(level).expect("compression level is clamped");
    Compressor::new(level)
}

pub(crate) fn zlib_compress(input: &[u8], level: u32) -> Result<Vec<u8>> {
    let mut compressor = compressor(level);
    let bound = compressor.zlib_compress_bound(input.len());
    let mut output = vec![0_u8; bound];
    let written = compressor
        .zlib_compress(input, &mut output)
        .map_err(|error| Error::invalid(format!("zlib compression failed: {error}")))?;
    output.truncate(written);
    Ok(output)
}

pub(crate) fn gzip_compress(input: &[u8], level: u32) -> Result<Vec<u8>> {
    let mut compressor = compressor(level);
    let bound = compressor.gzip_compress_bound(input.len());
    let mut output = vec![0_u8; bound];
    let written = compressor
        .gzip_compress(input, &mut output)
        .map_err(|error| Error::invalid(format!("gzip compression failed: {error}")))?;
    output.truncate(written);
    Ok(output)
}

pub(crate) fn zlib_decompress(input: &[u8], expected_len: usize) -> Result<Vec<u8>> {
    let mut decompressor = Decompressor::new();
    zlib_decompress_with(&mut decompressor, input, expected_len)
}

pub(crate) fn zlib_decompress_with(
    decompressor: &mut Decompressor,
    input: &[u8],
    expected_len: usize,
) -> Result<Vec<u8>> {
    let mut output = vec![0_u8; expected_len];
    let written = decompressor
        .zlib_decompress(input, &mut output)
        .map_err(|error| Error::decode(format!("zlib decompression failed: {error}")))?;
    if written != expected_len {
        return Err(Error::decode("zlib decompressed length mismatch"));
    }
    Ok(output)
}

pub(crate) fn gzip_decompress(input: &[u8], expected_len: usize) -> Result<Vec<u8>> {
    let mut decompressor = Decompressor::new();
    let mut output = vec![0_u8; expected_len];
    let written = decompressor
        .gzip_decompress(input, &mut output)
        .map_err(|error| Error::decode(format!("gzip decompression failed: {error}")))?;
    if written != expected_len {
        return Err(Error::decode("gzip decompressed length mismatch"));
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::{gzip_compress, gzip_decompress, zlib_compress, zlib_decompress};

    #[test]
    fn zlib_and_gzip_round_trip_edge_lengths() {
        for len in [0, 1, 31, 32, 33, 511, 512, 513, 32_767, 32_768, 32_769] {
            let input: Vec<u8> = (0..len)
                .map(|index| (index as u8).wrapping_mul(31).wrapping_add(17))
                .collect();

            let zlib = zlib_compress(&input, 4).expect("compress zlib");
            assert_eq!(
                zlib_decompress(&zlib, input.len()).expect("decompress zlib"),
                input
            );

            let gzip = gzip_compress(&input, 6).expect("compress gzip");
            assert_eq!(
                gzip_decompress(&gzip, input.len()).expect("decompress gzip"),
                input
            );
        }
    }

    #[test]
    fn decompression_rejects_wrong_declared_lengths() {
        let input = vec![0x5a; 4_096];
        let zlib = zlib_compress(&input, 4).expect("compress zlib");
        let gzip = gzip_compress(&input, 6).expect("compress gzip");

        for expected in [input.len() - 1, input.len() + 1] {
            assert!(zlib_decompress(&zlib, expected).is_err());
            assert!(gzip_decompress(&gzip, expected).is_err());
        }
    }
}
