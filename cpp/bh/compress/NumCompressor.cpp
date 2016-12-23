/* Copyright (C)  2005-2008 Infobright Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2.0 as
published by the Free  Software Foundation.

This program is distributed in the hope that  it will be useful, but
WITHOUT ANY WARRANTY; without even  the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License version 2.0 for more details.

You should have received a  copy of the GNU General Public License
version 2.0  along with this  program; if not, write to the Free
Software Foundation,  Inc., 59 Temple Place, Suite 330, Boston, MA
02111-1307 USA  */

//-------------------------------------------------------------------------

//TEMPLATE_CLS(NumCompressor)

//-------------------------------------------------------------------------


// TODO: detection of outliers, before PartDict
// TODO: uniform encoding of top bits, then again TopBitDict
// TODO: detection of repeated value
// TODO: DataFilt_Diff - save the 1st value separately, don't propagate it further
// TODO: passing column type (date/decimal/...) to Compress
// TODO: faster computation of GCD
// TODO: detection of decimal nature of integers
// TODO: time - use Huffman coding in PartDict
// TODO: ratio, time - store statistics about many packs, outside this routine

// TODO: (outside) don't reallocate NumCompressor for each pack; this gives more than 2-fold speed improvement !!!
// TODO: improve speed of BitstreamCompressor

// DONE: time - faster choice between differenced or non-differenced sequence, based on partial prediction
// DONE: time - in range coder, shift 'range' right instead of dividing by 'total' (also in EncodeUniform)
// DONE: try to use TopBitDict before PartDict
// DONE: external buffer
// DONE: template to process uchar, ushort and uint, not only uint64
// DONE: time - remove FreeBinTable outside this routine
// DONE: in TopBitDict::FindOptimum(), set nbit = GetBitLen(maxval), which can be larger than current value
// DONE: MinMax after DataFilt_Diff - no improvement
// DONE: Dictionary.cpp - different arguments to template, depending on uchar/uint/...
// DONE: encode lower bits of data, like the upper
// DONE: TopBitDict - improve speed; start with a dictionary for top 8 (or 12) bits
// DONE: DataFilt_Diff - compute histogram based on top (and bottom?) 8-12 bits of data, without sorting
// DONE: DataFilt_Uniform - improve for the case when data have integral bit-lenght
// DONE: PartDict - smart choice of dictionary size (no improvement)
