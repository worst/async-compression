use crate::{codec::Encode, unshared::Unshared, util::PartialBuffer};
use libzstd::stream::raw::{Encoder, Operation};
use std::io::Result;

#[derive(Debug)]
pub struct ZstdEncoder {
    encoder: Unshared<Encoder<'static>>,
}

impl ZstdEncoder {
    pub(crate) fn new(level: i32) -> Self {
        // We are going to try to hard code things to like, 4 threads. yolo.
        let mut encoder = Encoder::new(level).unwrap();
        let num_threads = zstd_safe::CParameter::NbWorkers(8);
        encoder.set_parameter(num_threads).unwrap();

        Self {
            // encoder: Unshared::new(Encoder::new(level).unwrap()),
            encoder: Unshared::new(encoder),
        }
    }
}

impl Encode for ZstdEncoder {
    fn encode(
        &mut self,
        input: &mut PartialBuffer<impl AsRef<[u8]>>,
        output: &mut PartialBuffer<impl AsRef<[u8]> + AsMut<[u8]>>,
    ) -> Result<()> {
        let status = self
            .encoder
            .get_mut()
            .run_on_buffers(input.unwritten(), output.unwritten_mut())?;
        input.advance(status.bytes_read);
        output.advance(status.bytes_written);
        Ok(())
    }

    fn flush(
        &mut self,
        output: &mut PartialBuffer<impl AsRef<[u8]> + AsMut<[u8]>>,
    ) -> Result<bool> {
        let mut out_buf = zstd_safe::OutBuffer::around(output.unwritten_mut());
        let bytes_left = self.encoder.get_mut().flush(&mut out_buf)?;
        let len = out_buf.as_slice().len();
        output.advance(len);
        Ok(bytes_left == 0)
    }

    fn finish(
        &mut self,
        output: &mut PartialBuffer<impl AsRef<[u8]> + AsMut<[u8]>>,
    ) -> Result<bool> {
        let mut out_buf = zstd_safe::OutBuffer::around(output.unwritten_mut());
        let bytes_left = self.encoder.get_mut().finish(&mut out_buf, true)?;
        let len = out_buf.as_slice().len();
        output.advance(len);
        Ok(bytes_left == 0)
    }
}
