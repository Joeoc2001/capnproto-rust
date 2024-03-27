// Copyright (c) 2013-2017 Sandstorm Development Group, Inc. and contributors
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

use core::slice;

use crate::message;
use crate::message::Allocator;
use crate::message::WriterSegment;
use crate::message::{ReaderSegment, ReaderSegments};
use crate::private::read_limiter::ReadLimiter;
use crate::private::units::*;
use crate::OutputSegments;
use crate::Result;

pub type SegmentId = u32;

/// A wrapper around a segment, carrying a reference to a [`ReadLimiter`] to limit reads
/// across the underlying segment collection.
pub struct ReaderArenaSegment<'a, S> {
    segment: S,
    read_limiter: &'a ReadLimiter,
}

impl<'a, S: ReaderSegment> ReaderSegment for ReaderArenaSegment<'a, S> {
    fn len_words(&self) -> usize {
        self.segment.len_words()
    }
}

/// A wrapper around some [`ReaderSegments`] with some additional limits to prevent denial of service.
/// Also implements [`ReaderSegments`].
pub struct ReaderArenaImpl<S> {
    segments: S,
    read_limiter: ReadLimiter,
    nesting_limit: i32,
}

#[cfg(feature = "sync_reader")]
fn _assert_sync() {
    fn _assert_sync<T: Sync>() {}
    fn _assert_reader<S: ReaderSegments + Sync>() {
        _assert_sync::<ReaderArenaImpl<S>>();
    }
}

impl<S> ReaderArenaImpl<S>
where
    S: ReaderSegments,
{
    pub fn new(segments: S, options: message::ReaderOptions) -> Self {
        let limiter = ReadLimiter::new(options.traversal_limit_in_words);
        Self {
            segments,
            read_limiter: limiter,
            nesting_limit: options.nesting_limit,
        }
    }

    pub fn into_segments(self) -> S {
        self.segments
    }
}

impl<S> ReaderSegments for ReaderArenaImpl<S>
where
    S: ReaderSegments,
{
    type Segment<'a> = ReaderArenaSegment<'a, S>;

    fn read_segment<'a>(&'a self, idx: u32) -> Option<Self::Segment<'a>> {
        let segment = self.segments.read_segment(idx)?;

        Some(ReaderArenaSegment {
            segment,
            read_limiter: &self.read_limiter,
        })
    }

    /*
    fn get_segment<'a>(&'a self, id: u32) -> Result<S::Segment<'a>> {
        match self.segments.read_segment(id) {
            Some(seg) => Ok(seg),
            None => Err(Error::from_kind(ErrorKind::InvalidSegmentId(id))),
        }
    }

    unsafe fn check_offset(
        &self,
        segment_id: u32,
        start: *const u8,
        offset_in_words: i32,
    ) -> Result<*const u8> {
        let (segment_start, segment_len) = self.get_segment(segment_id)?;
        let this_start: usize = segment_start as usize;
        let this_size: usize = segment_len as usize * BYTES_PER_WORD;
        let offset: i64 = i64::from(offset_in_words) * BYTES_PER_WORD as i64;
        let start_idx = start as usize;
        if start_idx < this_start || ((start_idx - this_start) as i64 + offset) as usize > this_size
        {
            Err(Error::from_kind(
                ErrorKind::MessageContainsOutOfBoundsPointer,
            ))
        } else {
            unsafe { Ok(start.offset(offset as isize)) }
        }
    }

    fn contains_interval(&self, id: u32, start: *const u8, size_in_words: usize) -> Result<()> {
        let (segment_start, segment_len) = self.get_segment(id)?;
        let this_start: usize = segment_start as usize;
        let this_size: usize = segment_len as usize * BYTES_PER_WORD;
        let start = start as usize;
        let size = size_in_words * BYTES_PER_WORD;

        if !(start >= this_start && start - this_start + size <= this_size) {
            Err(Error::from_kind(
                ErrorKind::MessageContainsOutOfBoundsPointer,
            ))
        } else {
            self.read_limiter.can_read(size_in_words)
        }
    }

    fn amplified_read(&self, virtual_amount: u64) -> Result<()> {
        self.read_limiter.can_read(virtual_amount as usize)
    }

    fn nesting_limit(&self) -> i32 {
        self.nesting_limit
    }
    */
}

pub trait BuilderArena<S: WriterSegment>: ReaderSegments {
    fn allocate(&mut self, segment_id: u32, amount: WordCount32) -> Option<u32>;
    fn allocate_anywhere(&mut self, amount: u32) -> (SegmentId, u32);
    fn get_segment_mut(&mut self, id: u32) -> (*mut u8, u32);
}

/// A wrapper around a memory segment used in building a message.
struct BuilderSegment<S> {
    /// Pointer to the start of the segment.
    ptr: S,

    /// Number of words already used in the segment.
    allocated: u32,
}

#[derive(Default)]
struct BuilderSegmentArray<S> {
    #[cfg(feature = "alloc")]
    segments: alloc::vec::Vec<BuilderSegment<S>>,

    // In the no-alloc case, we only allow a single segment.
    #[cfg(not(feature = "alloc"))]
    segment: Option<BuilderSegment<S>>,
}

impl<S> BuilderSegmentArray<S> {
    fn len(&self) -> usize {
        #[cfg(feature = "alloc")]
        return self.segments.len();

        #[cfg(not(feature = "alloc"))]
        return match self.segment {
            Some(_) => 1,
            None => 0,
        };
    }

    fn push(&mut self, segment: BuilderSegment) {
        #[cfg(feature = "alloc")]
        self.segments.push(segment);

        #[cfg(not(feature = "alloc"))]
        {
            if self.segment.is_some() {
                panic!("multiple segments are not supported in no-alloc mode")
            }
            self.segment = Some(segment);
        }
    }

    fn get(&self, index: usize) -> Option<&BuilderSegment> {
        #[cfg(feature = "alloc")]
        return self.segments.get(index);

        #[cfg(not(feature = "alloc"))]
        {
            return if index == 0 {
                self.segment.as_ref()
            } else {
                None
            };
        }
    }
}

pub struct BuilderArenaImplInner<A>
where
    A: Allocator,
{
    allocator: A,
    segments: BuilderSegmentArray<A::AllocatedSegment>,
}

pub struct BuilderArenaImpl<A>
where
    A: Allocator,
{
    inner: BuilderArenaImplInner<A>,
}

impl<A> BuilderArenaImpl<A>
where
    A: Allocator,
{
    pub fn new(allocator: A) -> Self {
        Self {
            inner: BuilderArenaImplInner {
                allocator,
                segments: Default::default(),
            },
        }
    }

    /// Allocates a new segment with capacity for at least `minimum_size` words.
    pub fn allocate_segment(&mut self, minimum_size: u32) -> Result<()> {
        self.inner.allocate_segment(minimum_size)
    }

    pub fn get_segments_for_output(&self) -> OutputSegments {
        let reff = &self.inner;
        if reff.segments.len() == 1 {
            let seg = &reff.segments[0];

            // The user must mutably borrow the `message::Builder` to be able to modify segment memory.
            // No such borrow will be possible while `self` is still immutably borrowed from this method,
            // so returning this slice is safe.
            let slice = unsafe {
                slice::from_raw_parts(seg.ptr as *const _, seg.allocated as usize * BYTES_PER_WORD)
            };
            OutputSegments::SingleSegment([slice])
        } else {
            #[cfg(feature = "alloc")]
            {
                let mut v = alloc::vec::Vec::with_capacity(reff.segments.len());
                for seg in &reff.segments {
                    // See safety argument in above branch.
                    let slice = unsafe {
                        slice::from_raw_parts(
                            seg.ptr as *const _,
                            seg.allocated as usize * BYTES_PER_WORD,
                        )
                    };
                    v.push(slice);
                }
                OutputSegments::MultiSegment(v)
            }
            #[cfg(not(feature = "alloc"))]
            {
                panic!("invalid number of segments: {}", reff.segments.len());
            }
        }
    }

    pub fn len(&self) -> usize {
        self.inner.segments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Retrieves the underlying `Allocator`, deallocating all currently-allocated
    /// segments.
    pub fn into_allocator(mut self) -> A {
        self.inner.deallocate_all();
        self.inner.allocator
    }
}

impl<A: Allocator> ReaderSegments for BuilderArenaImpl<A> {
    type Segment<'a> = &'a A::AllocatedSegment
    where
        Self: 'a;

    fn read_segment<'a>(&'a self, idx: SegmentId) -> Option<Self::Segment<'a>> {
        let segment = self.inner.segments.get(idx as usize)?;

        Some(&segment.ptr)
    }

    /*
    fn get_segment(&self, id: u32) -> Result<&[u8]> {
        let seg = match self.inner.segments.get(id as usize) {
            Some(seg) => seg,
            None => return Err(Error::from_kind(ErrorKind::InvalidSegmentId(id))),
        };

        let slice = unsafe { self.inner.allocator.read(seg.ptr, seg.allocated) };
        Ok(slice)
    }

    unsafe fn check_offset(
        &self,
        _segment_id: u32,
        start: *const u8,
        offset_in_words: i32,
    ) -> Result<*const u8> {
        unsafe { Ok(start.offset((i64::from(offset_in_words) * BYTES_PER_WORD as i64) as isize)) }
    }

    fn contains_interval(&self, _id: u32, _start: *const u8, _size: usize) -> Result<()> {
        Ok(())
    }

    fn amplified_read(&self, _virtual_amount: u64) -> Result<()> {
        Ok(())
    }

    fn nesting_limit(&self) -> i32 {
        0x7fffffff
    }
    */
}

impl<A> BuilderArenaImplInner<A>
where
    A: Allocator,
{
    /// Allocates a new segment with capacity for at least `minimum_size` words.
    fn allocate_segment(&mut self, minimum_size: WordCount32) -> Result<()> {
        let seg = self.allocator.try_allocate_segment(minimum_size);
        let (ptr, capacity) = seg?;
        self.segments.push(BuilderSegment { ptr, allocated: 0 });
        Ok(())
    }

    fn allocate(&mut self, segment_id: u32, amount: WordCount32) -> Option<u32> {
        let seg = &mut self.segments[segment_id as usize];
        if amount > seg.capacity - seg.allocated {
            None
        } else {
            let result = seg.allocated;
            seg.allocated += amount;
            Some(result)
        }
    }

    fn allocate_anywhere(&mut self, amount: u32) -> (SegmentId, u32) {
        // first try the existing segments, then try allocating a new segment.
        let allocated_len = self.segments.len() as u32;
        for segment_id in 0..allocated_len {
            if let Some(idx) = self.allocate(segment_id, amount) {
                return (segment_id, idx);
            }
        }

        // Need to allocate a new segment.

        self.allocate_segment(amount).expect("allocate new segment");
        (
            allocated_len,
            self.allocate(allocated_len, amount)
                .expect("use freshly-allocated segment"),
        )
    }

    fn deallocate_all(&mut self) {
        #[cfg(feature = "alloc")]
        for seg in &self.segments {
            unsafe {
                self.allocator
                    .deallocate_previous_segment(seg.ptr, seg.capacity, seg.allocated);
            }
        }

        #[cfg(not(feature = "alloc"))]
        if let Some(seg) = &self.segments.segment {
            unsafe {
                self.allocator
                    .deallocate_previous_segment(seg.ptr, seg.capacity, seg.allocated);
            }
        }
    }

    fn get_segment_mut(&mut self, id: u32) -> (*mut u8, u32) {
        let seg = &self.segments[id as usize];
        (seg.ptr, seg.capacity)
    }
}

impl<A> BuilderArena<A::AllocatedSegment> for BuilderArenaImpl<A>
where
    A: Allocator,
{
    fn allocate(&mut self, segment_id: u32, amount: WordCount32) -> Option<u32> {
        self.inner.allocate(segment_id, amount)
    }

    fn allocate_anywhere(&mut self, amount: u32) -> (SegmentId, u32) {
        self.inner.allocate_anywhere(amount)
    }

    fn get_segment_mut(&mut self, id: u32) -> (*mut u8, u32) {
        self.inner.get_segment_mut(id)
    }
}

impl<A> Drop for BuilderArenaImplInner<A>
where
    A: Allocator,
{
    fn drop(&mut self) {
        self.deallocate_all()
    }
}

pub struct NullArena;

impl ReaderSegments for NullArena {
    type Segment<'a> = &'a [u8]
    where
        Self: 'a;

    fn read_segment<'a>(&'a self, idx: u32) -> Option<Self::Segment<'a>> {
        None
    }

    /*
    fn get_segment(&self, _id: u32) -> Result<&[u8]> {
        Err(Error::from_kind(ErrorKind::TriedToReadFromNullArena))
    }

    unsafe fn check_offset(
        &self,
        _segment_id: u32,
        start: *const u8,
        offset_in_words: i32,
    ) -> Result<*const u8> {
        unsafe { Ok(start.add(offset_in_words as usize * BYTES_PER_WORD)) }
    }

    fn contains_interval(&self, _id: u32, _start: *const u8, _size: usize) -> Result<()> {
        Ok(())
    }

    fn amplified_read(&self, _virtual_amount: u64) -> Result<()> {
        Ok(())
    }

    fn nesting_limit(&self) -> i32 {
        0x7fffffff
    }
    */
}
