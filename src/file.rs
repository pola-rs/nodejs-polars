use napi::bindgen_prelude::{Buffer, BufferSlice, Function, JsObjectValue, Null, Object};
use napi::bindgen_prelude::ToNapiValue;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::Either;
use std::io::{Error, ErrorKind};
use std::io;
use std::io::Write;

pub struct JsFileLike<'a> {
    pub inner: Object<'a>,
    pub env: &'a napi::Env,
}

pub struct JsWriteStream<'a> {
    pub inner: Object<'a>,
    pub env: &'a napi::Env,
}

impl<'a> JsFileLike<'a> {
    pub fn new(inner: Object<'a>, env: &'a napi::Env) -> Self {
        JsFileLike { inner, env }
    }
}

impl<'a> JsWriteStream<'a> {
    pub fn new(inner: Object<'a>, env: &'a napi::Env) -> Self {
        JsWriteStream { inner, env }
    }
}
pub struct ThreadsafeWriteable {
    pub inner: ThreadsafeFunction<Either<Buffer, Null>>,
}

impl Write for ThreadsafeWriteable {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        let tsfn = &self.inner;
        tsfn.call(
            Ok(Either::A(buf.into())),
            ThreadsafeFunctionCallMode::Blocking,
        );
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        // JS write streams do not have a 'flush'
        Ok(())
    }
}
impl Write for JsFileLike<'_> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        let stream_write: Function = self.inner.get_named_property("push").unwrap();
        let bytes = BufferSlice::from_data(self.env, buf.to_owned()).unwrap();
        let js_buff = bytes.into_unknown(self.env).unwrap();
        stream_write.call(js_buff).unwrap();
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        // JS write streams do not have a 'flush'
        Ok(())
    }
}

impl Write for JsWriteStream<'_> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        let stream_write: Function = self.inner.get_named_property("write").unwrap();        
        let bytes = BufferSlice::from_data(self.env, buf.to_owned()).unwrap();
        let js_buff: Buffer = bytes.into_buffer(self.env).unwrap();
        let js_buff = js_buff.into_unknown(self.env).unwrap();
        let result = stream_write.apply(Some(&self.inner),js_buff);

        match result {
            Ok(_) => Ok(buf.len()),
            Err(e) => return Err(Error::new(ErrorKind::InvalidData, format!("Error writing: {:?}", e))),       
        }
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        // JS write streams do not have a 'flush'
        Ok(())
    }
}