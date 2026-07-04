//! Minimal LZ4 block-format codec (no external deps, rustc-1.75-friendly).
//! Greedy hash-chain matcher; byte-compatible with the LZ4 block spec.

const MIN_MATCH: usize = 4;
const HASH_LOG: usize = 16;

#[inline]
fn hash(seq: u32) -> usize {
    ((seq.wrapping_mul(2654435761)) >> (32 - HASH_LOG)) as usize
}

#[inline]
fn read_u32(b: &[u8], i: usize) -> u32 {
    u32::from_le_bytes(b[i..i + 4].try_into().unwrap())
}

fn put_len(out: &mut Vec<u8>, mut l: usize) {
    while l >= 255 {
        out.push(255);
        l -= 255;
    }
    out.push(l as u8);
}

pub fn compress_prepend_size(src: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(src.len() / 2 + 16);
    out.extend((src.len() as u32).to_le_bytes());
    if src.len() < 13 {
        // too small for matches: single literal run
        let t = (src.len().min(15) as u8) << 4;
        out.push(t);
        if src.len() >= 15 {
            put_len(&mut out, src.len() - 15);
        }
        out.extend(src);
        return out;
    }
    let mut table = vec![0u32; 1 << HASH_LOG];
    let mut i = 0usize;
    let mut anchor = 0usize;
    let match_limit = src.len() - 12; // spec: last match must start before end-12
    while i < match_limit {
        let h = hash(read_u32(src, i));
        let cand = table[h] as usize;
        table[h] = i as u32;
        if i > cand
            && i - cand <= 0xFFFF
            && read_u32(src, cand) == read_u32(src, i)
        {
            // extend match
            let mut ml = MIN_MATCH;
            let max = (src.len() - 5).saturating_sub(i); // keep 5 literal bytes at end
            while ml < max && src[cand + ml] == src[i + ml] {
                ml += 1;
            }
            let lit = i - anchor;
            let token_lit = lit.min(15) as u8;
            let token_match = (ml - MIN_MATCH).min(15) as u8;
            out.push((token_lit << 4) | token_match);
            if lit >= 15 {
                put_len(&mut out, lit - 15);
            }
            out.extend(&src[anchor..i]);
            out.extend(((i - cand) as u16).to_le_bytes());
            if ml - MIN_MATCH >= 15 {
                put_len(&mut out, ml - MIN_MATCH - 15);
            }
            i += ml;
            anchor = i;
        } else {
            i += 1;
        }
    }
    // trailing literals
    let lit = src.len() - anchor;
    let t = (lit.min(15) as u8) << 4;
    out.push(t);
    if lit >= 15 {
        put_len(&mut out, lit - 15);
    }
    out.extend(&src[anchor..]);
    out
}

pub fn decompress_size_prepended(src: &[u8]) -> Result<Vec<u8>, &'static str> {
    if src.len() < 4 {
        return Err("short");
    }
    let n = u32::from_le_bytes(src[..4].try_into().unwrap()) as usize;
    let mut out = Vec::with_capacity(n);
    let mut i = 4usize;
    while i < src.len() {
        let token = src[i];
        i += 1;
        let mut lit = (token >> 4) as usize;
        if lit == 15 {
            loop {
                let b = src[i];
                i += 1;
                lit += b as usize;
                if b != 255 {
                    break;
                }
            }
        }
        out.extend(&src[i..i + lit]);
        i += lit;
        if i >= src.len() {
            break; // last sequence has no match part
        }
        let off = u16::from_le_bytes(src[i..i + 2].try_into().unwrap()) as usize;
        i += 2;
        let mut ml = (token & 0xF) as usize;
        if ml == 15 {
            loop {
                let b = src[i];
                i += 1;
                ml += b as usize;
                if b != 255 {
                    break;
                }
            }
        }
        ml += MIN_MATCH;
        if off == 0 || off > out.len() {
            return Err("bad offset");
        }
        let start = out.len() - off;
        for k in 0..ml {
            let b = out[start + k];
            out.push(b);
        }
    }
    if out.len() != n {
        return Err("length mismatch");
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip() {
        for data in [
            b"".to_vec(),
            b"abc".to_vec(),
            b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_vec(),
            (0..10_000u32).flat_map(|x| (x % 97).to_le_bytes()).collect::<Vec<u8>>(),
        ] {
            let c = compress_prepend_size(&data);
            assert_eq!(decompress_size_prepended(&c).unwrap(), data);
        }
    }
}
