#ifndef DOMAININJECTION_H_INCLUDED
#define DOMAININJECTION_H_INCLUDED

#include <boost/shared_ptr.hpp>

#include "common/CommonDefinitions.h"
#include "system/MemoryManagement/TrackableObject.h"
#include "system/MemoryManagement/MMGuard.h"
#include "compress/NumCompressor.h"

enum DataBlockType { BLOCK_BINARY, 
					 BLOCK_STRING, 
					 BLOCK_NUMERIC_UCHAR,
					 BLOCK_NUMERIC_USHORT,
					 BLOCK_NUMERIC_UINT,
					 BLOCK_NUMERIC_UINT64
					};

class DataBlock : public TrackableObject
{
public:
	DataBlock(int capacity);
	virtual ~DataBlock() {};

	virtual DataBlockType	GetType() = 0;
	int				GetNObj()	{ return nobj; };
	virtual void	Compress(uint& buf_size) = 0;						
	virtual	void	Decompress(char* src_buf, uint src_buf_size) = 0;	
	virtual void	StoreCompressedData(char* dest_buf, uint buf_size) = 0;

	TRACKABLEOBJECT_TYPE TrackableType() const {return TO_DATABLOCK;}
protected:
	int				capacity;
	int				nobj;
};

class StringDataBlock : public DataBlock 
{
public:
	StringDataBlock(int capacity);
	virtual ~StringDataBlock() {};

	DataBlockType	GetType() { return BLOCK_STRING; };
	void	Add(char* ind, ushort len);
	ushort	GetLens(uint obj);
	char*	GetIndex(uint obj);
	uint	GetDataSize();

	void	Compress(uint& buf_size);
	void	Decompress(char* src_buf, uint src_buf_size);
	void	StoreCompressedData(char* dest_buf, uint buf_size);	

private:
	MMGuard<char*>	index;
	MMGuard<ushort>	lens;	
	ushort			max_len;	
	MMGuard<char>	comp_len_buf;
	uint			comp_len_buf_size;
	MMGuard<char>	comp_data_buf;
	int				comp_data_buf_size;
	MMGuard<char>	data;
	uint			data_size;

};

template<class T>
class NumericDataBlock : public DataBlock 
{
public:
	NumericDataBlock(int capacity);	
	~NumericDataBlock() {};

	DataBlockType	GetType();
	void	Add(T val);
	T		GetValue(uint obj);
	void	Compress(uint& buf_size);	
	void	StoreCompressedData(char* dest_buf, uint buf_size);	
	void	Decompress(char* src_buf, uint src_buf_size);

private:	
	MMGuard<T>		data;		
	T				max_val;
	MMGuard<char>	comp_data_buf;
	uint			comp_data_buf_size;
};

class BinaryDataBlock : public DataBlock 
{
public:
	BinaryDataBlock(int capacity);
	~BinaryDataBlock() {};

	DataBlockType	GetType() { return BLOCK_BINARY; };
	void	Compress(uint& buf_size);
	void	StoreCompressedData(char* dest_buf, uint buf_size);
	void	Decompress(char* src_buf, uint src_buf_size);
};

class CompressionStatistics
{
public:
	CompressionStatistics() : previous_no_obj(0), previous_no_outliers(0), new_no_obj(0), new_no_outliers(0) {}
	uint previous_no_obj;			// (input) index of a value from which the statistics are calculated
	uint previous_no_outliers;			// (output) number of outliers
	uint new_no_obj;
	uint new_no_outliers;
};

class DomainInjectionDecomposer
{
public:
	virtual ~DomainInjectionDecomposer() {}
	virtual void Decompose(StringDataBlock& in, std::vector<boost::shared_ptr<DataBlock> >& out, CompressionStatistics& stats) = 0;
	//virtual uint GetComposedSize(std::vector<boost::shared_ptr<DataBlock> >& in) = 0;
	virtual void Compose(std::vector<boost::shared_ptr<DataBlock> >& in, StringDataBlock& out, char* data, uint data_size, uint& outliers) = 0;

	virtual std::auto_ptr<DomainInjectionDecomposer> Clone() const = 0;
	virtual std::string GetRule() const = 0;
};

class DomainInjectionManager
{
public:
	DomainInjectionManager();
	virtual ~DomainInjectionManager();

	void SetPath(const std::string& path);
	void Init(const std::string& path);
	void Save();

	void SetTo(const std::string& decomposer_def);
	bool HasCurrent() const	{ return use_decomposition; };
	DomainInjectionDecomposer& GetCurrent();
	uint GetCurrentId();
	DomainInjectionDecomposer& Get(uint decomp_id);
	
	uint GetID(const std::string& decomposer_def) const;

	static std::auto_ptr<DomainInjectionDecomposer> GetDecomposer(const std::string& rule);
	static bool IsValid(const std::string& rule);

private:
	std::string path;
	std::vector<boost::shared_ptr<DomainInjectionDecomposer> > decomposers;
	bool use_decomposition;
};


inline void StringDataBlock::Add(char* ind, ushort len)
{
	BHASSERT(nobj<capacity, "Too many values added to string data block");
	index.get()[nobj] = ind;
	lens.get()[nobj] = len;
	if (len > max_len)
		max_len = len;
	nobj++;
};

inline ushort StringDataBlock::GetLens(uint obj)
{
	return lens.get()[obj];
};

inline char* StringDataBlock::GetIndex(uint obj)
{
	return index.get()[obj];
};

inline uint StringDataBlock::GetDataSize()
{
	return data_size;
};

template<class T> NumericDataBlock<T>::NumericDataBlock(int capacity) 
: DataBlock(capacity), data(), max_val(0)
{
	data = MMGuard<T>((T*)alloc(capacity * sizeof(T), BLOCK_TEMPORARY), *this);
}

template<class T> inline void NumericDataBlock<T>::Add(T val)
{	
	BHASSERT(nobj<capacity, "Too many values added to numeric data block");
	data.get()[nobj] = val;	
	if (val > max_val)
		max_val = val;
	nobj++;
};

template<class T> inline T NumericDataBlock<T>::GetValue(uint obj)
{	
	return data.get()[obj];
};

template<> inline DataBlockType NumericDataBlock<uchar>::GetType()
{
	return BLOCK_NUMERIC_UCHAR;
};

template<> inline DataBlockType NumericDataBlock<ushort>::GetType()
{
	return BLOCK_NUMERIC_USHORT;
};

template<> inline DataBlockType NumericDataBlock<uint>::GetType()
{
	return BLOCK_NUMERIC_UINT;
};

template<> inline DataBlockType NumericDataBlock<uint64>::GetType()
{
	return BLOCK_NUMERIC_UINT64;
};

template<class T> void NumericDataBlock<T>::Compress(uint& buf_size)
{
	bool cond = false;

	if(max_val != 0) {
		NumCompressor<T> nc(cond);
		comp_data_buf_size = nobj * sizeof(T) + 20;						
		comp_data_buf = MMGuard<char>((char*)alloc(comp_data_buf_size, BLOCK_TEMPORARY), *this);						
		CprsErr res = nc.Compress(comp_data_buf.get(), comp_data_buf_size, data.get(), nobj, max_val);
		if(res != CPRS_SUCCESS) {
			std::stringstream msg_buf;
			msg_buf << "Compression of numerical values in domain expert subcollection failed (error " << res << ").";
			throw InternalRCException(msg_buf.str());
		}
	}
	else
		comp_data_buf_size = 0;	
	
	buf_size =  sizeof(int) + sizeof(T) + comp_data_buf_size;
}

template<class T> void NumericDataBlock<T>::StoreCompressedData(char* dest_buf, uint buf_size) 
{
	BHASSERT(buf_size==sizeof(int)+sizeof(T)+comp_data_buf_size, "Wrong buffer size in NumericDataBlock::StoreCompressedData()");
	*((int*)(dest_buf)) = nobj;
	dest_buf += sizeof(int);
	*((T*)dest_buf) = max_val;
	dest_buf += sizeof(T);
	if(comp_data_buf_size)
		memcpy(dest_buf, comp_data_buf.get(), comp_data_buf_size);
}

template<class T> void NumericDataBlock<T>::Decompress(char* src_buf, uint src_buf_size)
{
	nobj = *((int*)(src_buf));
	BHASSERT(nobj<=capacity, "Too small buffers while decompressing data block");
	src_buf += sizeof(int);
	max_val = *((T*)src_buf);
	src_buf += sizeof(T);
	if (max_val!=0) {
		NumCompressor<T> nc;
		CprsErr res = nc.Decompress(data.get(), src_buf, src_buf_size-sizeof(int)-sizeof(T), nobj, max_val);
		if(res != CPRS_SUCCESS) {
			std::stringstream msg_buf;
			msg_buf << "Decompression of numerical values in domain expert subcollection failed (error " << res << ").";
			throw DatabaseRCException(msg_buf.str());
		}
	}
	else
		for (int i=0; i<nobj; i++)
			data.get()[i] = 0;
}

#endif
