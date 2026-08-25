//! SIMD-accelerated helpers used by the crate when the `simd` feature is enabled.

/// Attempts to pack an ASCII `'0'`/`'1'` bit string into its packed representation using architecture
/// specific SIMD instructions. Returns `None` if the architecture is unsupported, the required
/// hardware feature is unavailable at runtime, or an unsupported character is encountered.
pub(crate) fn pack_ascii_bits(data: &[u8], packed_len: usize) -> Option<Vec<u8>> {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("sse2") {
            // SAFETY: runtime feature detection guarantees SSE2 support for this call.
            unsafe {
                return pack_ascii_bits_x86_sse2(data, packed_len);
            }
        }
    }
    let _ = (data, packed_len);
    None
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn pack_ascii_bits_x86_sse2(data: &[u8], packed_len: usize) -> Option<Vec<u8>> {
    use std::arch::x86_64::{
        __m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8, _mm_or_si128, _mm_set1_epi8,
    };

    let mut out = vec![0u8; packed_len];
    let mut idx = 0usize;
    let mut out_index = 0usize;

    if data.is_empty() {
        return Some(out);
    }

    let zero = _mm_set1_epi8(b'0'.cast_signed());
    let one = _mm_set1_epi8(b'1'.cast_signed());

    while idx + 16 <= data.len() {
        // SAFETY: the loop condition proves that 16 readable bytes remain. The unaligned load
        // imposes no alignment requirement on the resulting pointer.
        let ptr = unsafe { data.as_ptr().add(idx).cast::<__m128i>() };
        let bytes = unsafe { _mm_loadu_si128(ptr) };
        let is_zero = _mm_cmpeq_epi8(bytes, zero);
        let is_one = _mm_cmpeq_epi8(bytes, one);
        let valid = _mm_or_si128(is_zero, is_one);
        if _mm_movemask_epi8(valid) != 0xFFFF {
            return None;
        }

        let mask = u16::try_from(_mm_movemask_epi8(is_one))
            .expect("SSE2 movemask always returns a 16-bit non-negative value");
        let [low, high] = mask.to_le_bytes();
        if out_index < packed_len {
            out[out_index] = low.reverse_bits();
            out_index += 1;
        }
        if out_index < packed_len {
            out[out_index] = high.reverse_bits();
            out_index += 1;
        }
        idx += 16;
    }

    while idx + 8 <= data.len() {
        let mut byte = 0u8;
        for &c in &data[idx..idx + 8] {
            if c != b'0' && c != b'1' {
                return None;
            }
            byte = (byte << 1) | u8::from(c == b'1');
        }
        if out_index < packed_len {
            out[out_index] = byte;
            out_index += 1;
        }
        idx += 8;
    }

    if idx < data.len() {
        let mut byte = 0u8;
        for (offset, &c) in data[idx..].iter().enumerate() {
            if c != b'0' && c != b'1' {
                return None;
            }
            if c == b'1' {
                byte |= 1 << (7 - offset);
            }
        }
        if out_index < packed_len {
            out[out_index] = byte;
        }
    }

    Some(out)
}
