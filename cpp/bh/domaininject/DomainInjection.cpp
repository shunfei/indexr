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

#include <fstream>
#include <string>
#include <iostream>

#include <boost/shared_ptr.hpp>

#include "DomainInjection.h"
#include "system/MemoryManagement/MMGuard.h"
#include "compress/BitstreamCompressor.h"
#include "compress/TextCompressor.h"
#include "compress/PartDict.h"
#include "Halver.h"
#include "IPv4_Decomposer.h"
#include "IPv4_Decomposer2.h"
#include "Concatenator.h"
#include "DomainInjectionsDictionary.h"

using namespace std;
using namespace boost;

DataBlock::DataBlock(int capacity) : capacity(capacity), nobj(0)
{

}

StringDataBlock::StringDataBlock(int capacity) 
: DataBlock(capacity), index(), lens(), max_len(0), comp_data_buf_size(0), data_size(0)
{
	index = MMGuard<char*>((char**)alloc(capacity * sizeof(char*), BLOCK_TEMPORARY), *this);
	lens = MMGuard<ushort>((ushort*)alloc(capacity * sizeof(ushort), BLOCK_TEMPORARY), *this);
}

BinaryDataBlock::BinaryDataBlock(int capacity) : DataBlock(capacity)
{

}


void StringDataBlock::Compress(uint& buf_size)
{
	comp_data_buf_size = 0;

	data_size = 0;	
	for(int i = 0; i < nobj; i++)
		data_size += lens[i];

	bool cond = false; //inserting_mode == 1 && no_obj < 0x10000;

	// LENS

	if(max_len > 0) {	
		comp_len_buf_size = nobj * sizeof(uint) + 2 * sizeof(uint) + 28;
		comp_len_buf = MMGuard<char>((char*) alloc(comp_len_buf_size, BLOCK_TEMPORARY), *this);
		NumCompressor<ushort> nc(cond);
		CprsErr res = nc.Compress(comp_len_buf.get() + 2 * sizeof(uint), comp_len_buf_size, lens.get(), nobj, max_len);
		if(res != CPRS_SUCCESS) {
			std::stringstream msg_buf;
			msg_buf << "Compression of lengths of string values in domain expert subcollection failed (error " << res << ").";
			throw InternalRCException(msg_buf.str());
		}
		*(uint*) comp_len_buf.get() = comp_len_buf_size;
		*(uint*)(comp_len_buf.get() + sizeof(uint)) = max_len;

		// DATA
		comp_data_buf_size = data_size + 10;	
		comp_data_buf = MMGuard<char>((char*) alloc(comp_data_buf_size, BLOCK_TEMPORARY), *this);

		TextCompressor tc;			
		res = tc.Compress(comp_data_buf.get(), comp_data_buf_size, index.get(), lens.get(), nobj, cond ? 0 : TextCompressor::VER);
		if(res != CPRS_SUCCESS) {
			std::stringstream msg_buf;
			msg_buf << "Compression of string values in domain expert subcollection failed (error " << res << ").";
			throw InternalRCException(msg_buf.str());
		}

	} else {
		comp_len_buf_size = 0;
		comp_len_buf = MMGuard<char>((char*) alloc(2 * sizeof(uint), BLOCK_TEMPORARY), *this);
		*(uint*) comp_len_buf.get() = 0;
		*(uint*)(comp_len_buf.get() + sizeof(uint)) = max_len;
		comp_data_buf_size = 0;
	}

	buf_size = comp_len_buf_size + comp_data_buf_size + 4 * sizeof(uint);
	
}

void	StringDataBlock::StoreCompressedData(char* dest_buf, uint buf_size) 
{
	BHASSERT(buf_size==2*sizeof(int)+8+comp_len_buf_size+comp_data_buf_size, "Wrong buffer size in StringDataBlock::StoreCompressedData()");
	// Copy buffer
	*((int*) dest_buf) = nobj;
	dest_buf += sizeof(int);

	*((uint*) dest_buf) = data_size;
	dest_buf += sizeof(uint);

	memcpy(dest_buf, comp_len_buf.get(), comp_len_buf_size + 8);
	dest_buf += comp_len_buf_size + 8;

	if(comp_data_buf_size)
		memcpy(dest_buf, comp_data_buf.get(), comp_data_buf_size);
	dest_buf += comp_data_buf_size;
}

void StringDataBlock::Decompress(char* src_buf, uint src_buf_size)
{
	nobj = *((int*)(src_buf));
	BHASSERT(nobj<=capacity, "Too small buffers while decompressing data block");
	int shift = sizeof(int);
	data_size = *((uint*)(src_buf + shift));
	shift += sizeof(uint);
	comp_len_buf_size = *((uint*)(src_buf + shift));
	shift += sizeof(uint);
	max_len = *((uint*)(src_buf + shift));
	shift += sizeof(uint);
	
	if(max_len > 0) {
		NumCompressor<ushort> nc;
		CprsErr res = nc.Decompress(lens.get(), src_buf + shift, comp_len_buf_size, nobj, (ushort) max_len);
		if(res != CPRS_SUCCESS) {
			std::stringstream msg_buf;
			msg_buf << "Decompression of lengths of string values in domain expert subcollection failed (error " << res << ").";
			throw DatabaseRCException(msg_buf.str());
		}
		TextCompressor tc;		
		data = MMGuard<char>((char*) alloc(data_size, BLOCK_TEMPORARY), *this);
		if(nobj) {
			res = tc.Decompress(data.get(), data_size, src_buf + shift + comp_len_buf_size, src_buf_size - shift - comp_len_buf_size, index.get(), lens.get(), nobj);
			if(res != CPRS_SUCCESS) {
				std::stringstream msg_buf;
				msg_buf << "Decompression of string values in domain expert subcollection failed (error " << res << ").";
				throw DatabaseRCException(msg_buf.str());
			}
		}
	} else {
		for(int i = 0; i<nobj; i++) {
			index.get()[i] = NULL;
			lens.get()[i] = 0;
		}
	}	
}

void BinaryDataBlock::Compress(uint& buf_size)
{
	BHASSERT(0, "Binary data block not implemented yet");
}

void BinaryDataBlock::StoreCompressedData(char* dest_buf, uint buf_size) 
{
	BHASSERT(0, "Binary data block not implemented yet");
}

void BinaryDataBlock::Decompress(char* src_buf, uint src_buf_size)
{
	BHASSERT(0, "Binary data block not implemented yet");
}

DomainInjectionManager::DomainInjectionManager() : use_decomposition(false)
{
	decomposers.push_back(shared_ptr<DomainInjectionDecomposer>());
}

DomainInjectionManager::~DomainInjectionManager()
{
}

void DomainInjectionManager::SetPath(const std::string& path)
{
	this->path = path;
}

void DomainInjectionManager::Init(const std::string& path)
{
	char buf[1024];
	this->path = path;
	std::ifstream infile(path.c_str());
	if (infile.is_open()) {
		while (infile.getline(buf, 1024).good())
			decomposers.push_back(boost::shared_ptr<DomainInjectionDecomposer>(GetDecomposer(buf)));
	}
}

void DomainInjectionManager::Save()
{
	if (decomposers.size()>1) {
		std::ofstream outfile(path.c_str());
		for (std::vector<shared_ptr<DomainInjectionDecomposer> >::const_iterator it = decomposers.begin(); it != decomposers.end(); it++)
			if (*it)
				outfile << (*it)->GetRule() << std::endl;
	}
}

void DomainInjectionManager::SetTo(const std::string& decomposer_def)
{
	if (decomposer_def.length()==0) {
		if (use_decomposition) {
			use_decomposition = false;
		}
	} else {
		if (decomposers.size()<=1 || decomposer_def!=decomposers.back()->GetRule()) {
			decomposers.push_back(boost::shared_ptr<DomainInjectionDecomposer>(GetDecomposer(decomposer_def)));
		}
		use_decomposition = true;
	}
}

std::auto_ptr<DomainInjectionDecomposer> DomainInjectionManager::GetDecomposer(const std::string& rule)
{
	if(rule.compare(Halver::GetName()) == 0)
		return std::auto_ptr<DomainInjectionDecomposer>(new Halver());
	if(rule.compare(IPv4_Decomposer::GetName()) == 0)
		return std::auto_ptr<DomainInjectionDecomposer>(new IPv4_Decomposer());
	if(rule.compare(IPv4_Decomposer2::GetName()) == 0)
		return std::auto_ptr<DomainInjectionDecomposer>(new IPv4_Decomposer2());
	return std::auto_ptr<DomainInjectionDecomposer>(new Concatenator(rule));
}

bool DomainInjectionManager::IsValid(const std::string& rule)
{
	return (int)Concatenator::IsValid(rule);
}

DomainInjectionDecomposer& DomainInjectionManager::GetCurrent()
{
	BHASSERT(use_decomposition, "Decomposition rule not set for a column");
	return *(decomposers.back());
}

uint DomainInjectionManager::GetCurrentId()
{
	return (use_decomposition ? uint(decomposers.size() - 1) : 0);
}

DomainInjectionDecomposer& DomainInjectionManager::Get(uint decomp_id)
{
	return *(decomposers[decomp_id]);
}

uint DomainInjectionManager::GetID(const std::string& decomposer_def) const
{
	if(decomposer_def.empty())
		return 0;
	for(int i = int(decomposers.size()) - 1; i > 0; --i) {
		if(decomposers[i]->GetRule() == decomposer_def)
			return i;
	}
	throw InternalRCException("Decomposition rule " + decomposer_def + " not found");
}
