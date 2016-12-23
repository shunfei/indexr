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

#include "core/Filter.h"
#include "core/PackOrderer.h"

FilterOnesIterator::FilterOnesIterator() : buffer(max_ahead)
{
	valid = false;
	f = NULL;
	cur_position = -1;
	b = 0;
	bln = 0;
	lastn = 0;
	bitsLeft = 0;
	prev_block = -1;
	iterator_n = -1;
	iterator_b = prev_iterator_b = -1;
	cur_block_full = false;
	cur_block_empty = false;
	inside_one_pack = true;
	packs_to_go = -1; //all packs
	packs_done = 0;
}

FilterOnesIterator::FilterOnesIterator(Filter *ff) : buffer(max_ahead)
{
	Init(ff);
}

void FilterOnesIterator::Init(Filter *ff)		//!< reinitialize iterator (possibly with the new filter)
{
	f = ff;
	packs_done = 0; //all packs
	packs_to_go = -1; //all packs
	bool nonempty_pack_found = false;
	iterator_b = prev_iterator_b = 0;
	inside_one_pack = true;
	while(iterator_b < f->no_blocks) { 			// check whether there is more than one pack involved
		if(f->block_status[iterator_b] != f->FB_EMPTY) {
			if(nonempty_pack_found) {
				inside_one_pack = false;
				break;
			}
			nonempty_pack_found = true;
		}
		iterator_b++;
	}
	FilterOnesIterator::Rewind();									//!< initialize on the beginning
}

void FilterOnesIterator::SetNoPacksToGo(int n)
{
	packs_to_go = n;
	packs_done = 0;
}

void FilterOnesIterator::Reset()
{
    cur_position = -1;
    b = 0;
	bln = 0;
	lastn = 0;
	bitsLeft = 0;
    prev_block = -1;
    valid = true;
    iterator_n = 65535;
    iterator_b = prev_iterator_b = -1;
    buffer.Reset();
}

void FilterOnesIterator::Rewind()				//!< iterate from the beginning (the first 1 in Filter)
{
    Reset();
    FilterOnesIterator::operator++();
}

void FilterOnesIterator::RewindToRow(const _int64 row)	// note: if row not exists , set IsValid() to false
{
	int pack = int(row >> 16);
	if(pack >= f->no_blocks ||
			(pack == f->no_blocks-1 && (row & 0xffff) >= f->no_of_bits_in_last_block))
		valid = false;
	else
		valid = true;
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(pack >= 0 && pack < f->no_blocks);

	iterator_b = prev_iterator_b = pack ;
	iterator_n = int(row & 0xffff);
	uchar stat = f->block_status[iterator_b];
	cur_block_full = (stat == f->FB_FULL);
	cur_block_empty = (stat == f->FB_EMPTY);
	buffer.Reset();			// random order - forget history
	if(cur_block_empty) {
		valid = false;
		return;
	}
	packs_done = 0;
	if(stat == f->FB_MIXED)
		if(!f->Get(row))
			valid = false;
		else
			ones_left_in_block = f->blocks[iterator_b]->no_set_bits;
	cur_position = ((_int64(iterator_b)) << 16) + iterator_n;
}

bool FilterOnesIterator::RewindToPack(int pack)
{
	// if SetNoPacksToGo() has been used, then RewindToPack can be done only to the previous pack
	assert(packs_to_go == -1 || pack == prev_iterator_b || pack == iterator_b);
	if(pack >= f->no_blocks)
		valid = false;
	else
		valid = true;
	if(iterator_b != pack)
		packs_done--;

	iterator_b = prev_iterator_b = pack;
	prev_block = pack;
	b = 0;
	bitsLeft = 0;
	lastn = -2;
	bln = 0; 
	iterator_n = 0;
	uchar stat = f->block_status[iterator_b];
	cur_block_full = (stat == f->FB_FULL);
	cur_block_empty = (stat == f->FB_EMPTY);
//	assert(buffer.Empty());			// random order - forget history, WARNING: may lead to omitting packs!
	buffer.Reset();
	if(cur_block_empty) {
		NextPack();
		return false;
	}
	if(!cur_block_full) {
		iterator_n = -1;
		FilterOnesIterator::operator++();
		ones_left_in_block = f->blocks[iterator_b]->no_set_bits;
	}
	cur_position = ((_int64(iterator_b)) << 16) + iterator_n;
	return true;
}


void FilterOnesIterator::NextPack()
{
	iterator_n = 65535;
	FilterOnesIterator::operator++();
}

_int64 FilterOnesIterator::GetPackSizeLeft()	// how many 1-s in the current block left (including the current one)
{
	if(!valid)
		return 0;
	if(cur_block_full)
		return _int64(f->block_last_one[iterator_b]) + 1 - iterator_n;	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(f->block_status[iterator_b] != f->FB_EMPTY);
	return ones_left_in_block;
}

int FilterOnesIterator::Lookahead(int n)
{
	if(!IsValid())
		return -1;
	if(n ==0)
		return iterator_b;

	if(buffer.Elems() >= n)
		return buffer.Nth(n-1);

	int tmp_b = buffer.Empty() ? iterator_b : buffer.GetLast();
	n -= buffer.Elems();

	while(n>0) {
		++tmp_b;
		while(tmp_b < f->no_blocks && f->block_status[tmp_b] == f->FB_EMPTY)
			++tmp_b;
		if(tmp_b < f->no_blocks) {
			n--;
			buffer.Put(tmp_b);
		} else
			return  buffer.Empty() ? iterator_b : buffer.GetLast();
	}
	return tmp_b;
}


bool FilterOnesIterator::IteratorBpp()
{
	iterator_n = 0;
	prev_iterator_b = iterator_b;
	if(buffer.Empty()) {
		iterator_b++;
		while(iterator_b < f->no_blocks && f->block_status[iterator_b] == f->FB_EMPTY)
			iterator_b++;
		if(iterator_b >= f->no_blocks)
			return false;
	} else
		iterator_b = buffer.Get();

	uchar stat = f->block_status[iterator_b];
	cur_block_full = (stat == f->FB_FULL);
	cur_block_empty = (stat == f->FB_EMPTY);
	if(packs_to_go == -1 || ++packs_done < packs_to_go)
		return true;
	else
		return false;
}

bool FilterOnesIterator::NextInsidePack()	// return false if we just restarted the pack after going to its end
{
	bool inside_pack = true;
	if(IsEndOfBlock()) {
		inside_pack = false;
		iterator_n = 0;
		prev_block = -1;
	} else
		iterator_n++; // now we are on the first suspected position - go forward if it is not 1
	bool found = false;
	while(!found) {
		if(cur_block_full) {
			found = true;
		} else if(cur_block_empty) { //Updating iterator can cause this
			return false;
		} else {
			found = FindOneInsidePack();
			if(!found) {
				inside_pack = false;
				iterator_n = 0;
				prev_block = -1;
			}
		}
	}
	cur_position = ((_int64(iterator_b)) << 16) + iterator_n;
	return inside_pack;
}

FilterOnesIterator& FilterOnesIterator::operator++()
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(valid);		// cannot iterate invalid iterator
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(packs_to_go==-1 || packs_to_go > packs_done);

	if(IsEndOfBlock()) {
		if(!IteratorBpp()) {
			valid = false;
			return *this;
		}
	} else
		iterator_n++; // now we are on the first suspected position - go forward if it is not 1

	bool found = false;
	while(!found) {
		if(cur_block_full) {
			found = true;
		} else if(cur_block_empty) { //Updating iterator can cause this
			if(!IteratorBpp()) {
				valid = false;
				return *this;
			}
		} else {
			found = FindOneInsidePack();
			if(!found) {
				if(!IteratorBpp()) {
					valid = false;
					return *this;
				}
			}
		}
	}
	cur_position = ((_int64(iterator_b)) << 16) + iterator_n;
	return *this;
}

bool FilterOnesIterator::FindOneInsidePack()
{
	bool found = false;
	///////////////////// inter-block iteration //////////////////////////
	assert(f->block_status[iterator_b] == f->FB_MIXED); //IteratorBpp() omits empty blocks
	Filter::Block *cur_block = f->blocks[iterator_b];
	int cb_no_obj = cur_block->no_obj;
	if(iterator_b == prev_block) {
		ones_left_in_block--;		// we already were in this block and the previous bit is after us
	} else {
		b = 0;
		bitsLeft = 0;
		lastn = -2;
		bln = 0;
		prev_block = iterator_b;
		ones_left_in_block = cur_block->no_set_bits;
	}
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(iterator_n < cb_no_obj || ones_left_in_block == 0);
	if(iterator_n < cb_no_obj) {
		// else leave "found == false", which will iterate to the next block
		if(lastn + 1 == iterator_n) {
			if(bitsLeft > 0)
				bitsLeft--;
			b >>= 1;
		} else {
			bln = iterator_n >> 5;
			int bitno = iterator_n & 0x1f;
			b = cur_block->block_table[bln];
			b >>= bitno;
			bitsLeft = 32 - bitno;
		}
		// find bit == 1
		while(!found && iterator_n < cb_no_obj) {
			// first find a portion with 1
			if(b == 0) {
				iterator_n += bitsLeft;
				if(++bln >= cur_block->block_size)
					break;				// leave found == false
				while(cur_block->block_table[bln] == 0) {
					iterator_n += 32;
					if(iterator_n >= cb_no_obj)
						break;
					++bln;
				}
				if(iterator_n >= cb_no_obj)
					break;
				b = cur_block->block_table[bln];
				bitsLeft = 32;
			}

			while(iterator_n < cb_no_obj) {
				// non-zero value found - process 32bit portion
				int skip = cur_block->posOf1[b & 0xff];
				if(skip > bitsLeft) {
					//end of 32bit portion
					iterator_n += bitsLeft;
					break;
				} else {
					iterator_n += skip;
					if(iterator_n >= cb_no_obj)
						break;
					bitsLeft -= skip;
					b >>= skip;
					if(skip < 8) {
						// found 1
						lastn = iterator_n;
						found = true;
						break;
					}
				}
			}
		}
	}
	//////////////////////////////////////////////////////////////////////
	return found;
}

FilterOnesIterator* FilterOnesIterator::Copy(int packs_to_go)
{
	FilterOnesIterator* f = new FilterOnesIterator();
	*f = *this;
	f->packs_to_go = packs_to_go;
	return f;
}


///////////////////////////////////////////////////////////////////////////////////////

FilterOnesIteratorOrdered::FilterOnesIteratorOrdered() : FilterOnesIterator(), po(NULL) {};

FilterOnesIteratorOrdered::FilterOnesIteratorOrdered(Filter *ff, PackOrderer* po)
{
	Init(ff,po);
}

void FilterOnesIteratorOrdered::Init(Filter *ff, PackOrderer* _po)
{
	po = _po;
	Reset();
	po->Rewind();
	FilterOnesIterator::Init(ff);
}

void FilterOnesIteratorOrdered::Rewind()
{
    Reset();
    po->Rewind();
    FilterOnesIteratorOrdered::operator++();
}



int FilterOnesIteratorOrdered::Lookahead(int n)
{
	if(n == 0)
		return iterator_b;

	if(buffer.Elems() >= n)
		return buffer.Nth(n-1);

	int tmp_b = po->Current();
	n -= buffer.Elems();

	while(n>0) {
		tmp_b = (++(*po)).Current();
		while(po->IsValid() && f->block_status[tmp_b] == f->FB_EMPTY)
			tmp_b = (++(*po)).Current();
		if(po->IsValid()) {
			n--;
			buffer.Put(tmp_b);
		} else
			return  buffer.Empty() ? iterator_b : buffer.GetLast();
	}
	return tmp_b;
}

bool 	FilterOnesIteratorOrdered::NaturallyOrdered()			
{ 
	return po->NaturallyOrdered(); 
}

bool FilterOnesIteratorOrdered::IteratorBpp()
{
	iterator_n = 0;
	prev_iterator_b = iterator_b;
	if(buffer.Empty()) {
		iterator_b = (++(*po)).Current();
		while(po->IsValid() && f->block_status[iterator_b] == f->FB_EMPTY)
			iterator_b = (++(*po)).Current();
		if(!po->IsValid())
			return false;
	} else
		iterator_b = buffer.Get();
	
	uchar stat = f->block_status[iterator_b];
	cur_block_full = (stat == f->FB_FULL);
	cur_block_empty = (stat == f->FB_EMPTY);
	if(packs_to_go == -1 || ++packs_done < packs_to_go)
		return true;
	else
		return false;
}

FilterOnesIteratorOrdered* FilterOnesIteratorOrdered::Copy()
{
	FilterOnesIteratorOrdered* f = new FilterOnesIteratorOrdered();
	*f = *this;
	f->po = new PackOrderer();
	*(f->po) = *po;
	return f;
}

void FilterOnesIteratorOrdered::NextPack()
{
	if(IteratorBpp()) {
		if(cur_block_full) {
			cur_position = ((_int64(iterator_b)) << 16) + iterator_n;
			return;
		} else {
			ones_left_in_block = f->blocks[iterator_b]->no_set_bits;
			if(f->blocks[iterator_b]->block_table[0] & 1)
				cur_position = ((_int64(iterator_b)) << 16) + iterator_n;
			else
				FilterOnesIteratorOrdered::operator++();
		}
	} else
		valid = false;
}


///////////////////////////////////////////////////////////////////////////////////////
