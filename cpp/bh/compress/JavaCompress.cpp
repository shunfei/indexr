#include <algorithm>

#include "JavaCompress.h"
#include "NumCompressor.h"
#include "TextCompressor.h"

#include "defs.h"
#include "system/fet.h"

using namespace std;

extern "C" void Test(java_long a){
	printf("a: %llu\n", a);
}

// ------------------------------------------------------------------------

// data:
// 
// | n0 | n1 | n2 | n3 |
// 
// cmp_data:
// 
// | err | max_val | cmp_data_size |      cmp_data    |
//    1      8           4            cmp_data_size
//
template<typename etype> char* JavaCompress_Number(void* data, unsigned int item_size, _uint64 max_val, NumCompressor<etype>& compressor){
	uint cmp_buffer_size = (item_size * 1 + 20) * sizeof(etype);
	uint cmp_data_size = cmp_buffer_size;
	int header_size = 1 + 8 + 4;
	int buffer_size = header_size + cmp_buffer_size;

	char* buffer = (char*)malloc(buffer_size); 
	char* cmp_data_buffer = buffer + header_size;

	CprsErr err = compressor.Compress(cmp_data_buffer, cmp_data_size, (etype*)data, item_size, (etype)max_val);
	*buffer = (char)err;
	if (err != CPRS_SUCCESS){
		return buffer;
	}

	*(_uint64*)(buffer + 1) = (_uint64)max_val;
	*(int*)(buffer + 9) = (int)cmp_data_size;

	return buffer;
}

template<typename etype> char* JavaDecompress_Number(char* cmp_data, unsigned int item_size, NumCompressor<etype>& compressor){
	// Check the compressed data.
	CprsErr data_err = (CprsErr)(*cmp_data);
	if (data_err != CPRS_SUCCESS){
		return 0;
	}

	int header_size = 1 + 8 + 4;
	_uint64 max_val = *(_uint64*)(cmp_data + 1);
	int cmp_data_size = *(int*)(cmp_data + 9);
	char* real_cmp_data = cmp_data + header_size;
	etype* data_buffer = (etype*)malloc(item_size * sizeof(etype));

	CprsErr err = compressor.Decompress(data_buffer, real_cmp_data, cmp_data_size, item_size, (etype)max_val);
	if(err != CPRS_SUCCESS){
		free(data_buffer);

		printf("JavaDecompress_Number error: %d\n", err);

		return 0;
	}
	
	return (char*)data_buffer;
}

extern "C" void* JavaCompress_Number_Byte(void* data, unsigned int item_size, unsigned long long max_val){
	NumCompressor<uchar> compressor;
	return JavaCompress_Number(data, item_size, max_val, compressor);
}

extern "C" void* JavaCompress_Number_Short(void* data, unsigned int item_size, unsigned long long max_val){
	NumCompressor<ushort> compressor;
	return JavaCompress_Number(data, item_size, max_val, compressor);
}

extern "C" void* JavaCompress_Number_Int(void* data, unsigned int item_size, unsigned long long max_val){
	NumCompressor<uint> compressor;
	return JavaCompress_Number(data, item_size, max_val, compressor);
}

extern "C" void* JavaCompress_Number_Long(void* data, unsigned int item_size, unsigned long long max_val){
	NumCompressor<_uint64> compressor;
	return JavaCompress_Number(data, item_size, max_val, compressor);
}


extern "C" void* JavaDecompress_Number_Byte(void* cmp_data, unsigned int item_size){
	NumCompressor<uchar> compressor;
	return JavaDecompress_Number((char*)cmp_data, item_size, compressor);
}

extern "C" void* JavaDecompress_Number_Short(void* cmp_data, unsigned int item_size){
	NumCompressor<ushort> compressor;
	return JavaDecompress_Number((char*)cmp_data, item_size, compressor);
}

extern "C" void* JavaDecompress_Number_Int(void* cmp_data, unsigned int item_size){
	NumCompressor<uint> compressor;
	return JavaDecompress_Number((char*)cmp_data, item_size, compressor);
}

extern "C" void* JavaDecompress_Number_Long(void* cmp_data, unsigned int item_size){
	NumCompressor<_uint64> compressor;
	return JavaDecompress_Number((char*)cmp_data, item_size, compressor);
}

// ------------------------------------------------------------------------

// data:
// 
// | s0 | s1 | s2 | s3 |
//
// cmp_data:
//
// | err | total_len | cmp_data_size |     cmp_data    |
//    1       4             4            cmp_data_size
//
char* JavaCompress_String(char** data_index, const ushort* lens, uint item_size, uint total_len){
	int cmp_buffer_size = total_len + 10;
	int cmp_data_size = cmp_buffer_size;

	int header_size = 1 + 4 + 4;
	int buffer_size = header_size + cmp_buffer_size;

	char* buffer = (char*)malloc(buffer_size);
	char* cmp_data_buffer = buffer + header_size;

	TextCompressor tc;			
	CprsErr err = tc.Compress(cmp_data_buffer, cmp_data_size, data_index, lens, item_size);
	*buffer = (char)err;
	if (err != CPRS_SUCCESS){
		return buffer;
	}

	*(uint*)(buffer + 1) = total_len;
	*(int*)(buffer + 5) = cmp_data_size;

	return buffer;
}


char* JavaDecompress_String(char* cmp_data, char** str_index, const ushort* lens, uint item_size){
	CprsErr data_err = (CprsErr)*cmp_data;
	if (data_err != CPRS_SUCCESS){
		printf("JavaDecompress_String data error: %d\n", data_err);
		return 0;
	}

	int header_size = 1 + 4 + 4;
	uint total_len = *(uint*)(cmp_data + 1);
	int cmp_data_size = *(int*)(cmp_data + 5);

	char* real_cmp_data = cmp_data + header_size;
	// char** str_index = (char**)malloc(item_size * sizeof(char*));
	char* buffer = (char*)malloc(total_len);

	TextCompressor tc;
	CprsErr err = tc.Decompress(buffer, total_len, real_cmp_data, cmp_data_size, str_index, lens, item_size);

	// free(str_index);

	if (err != CPRS_SUCCESS){
		free(buffer);

		printf("JavaDecompress_String error: %d\n", err);

		return 0;
	} else {
		return buffer;
	}
}


// data:
// | str_total_len | start0 | end0 | start1 | end1 | start2 | end2 | s0 | s1 | s2 |
//         4       | <-                index(int)               -> |<- str_data ->|
//
// cmp_data:
// | err | max_len | index_cmp_data_size | str_cmp_data_size |     index_cmp_data     |         str_cmp_data         |
//    1       2             4                       4            index_cmp_data_size       str_cmp_data_size
// | <-                cmp_header                         -> |
//
extern "C" void* JavaCompress_IndexedString(void* data, unsigned int item_size){
	char* _data = (char*)data + 4;

	uint* str_start_ends = (uint*)_data;
	int start_end_size = item_size * sizeof(uint) << 1;

	char** str_data_index = (char**)malloc(item_size * sizeof(char*));
	ushort* str_lens = (ushort*)malloc(item_size * sizeof(ushort));

	uint str_total_len = 0;
	ushort max_len = 0;
	
	for(int i = 0; i < item_size; i ++){
		uint start = str_start_ends[i << 1];
		uint end = str_start_ends[(i << 1) + 1];

		str_data_index[i] = _data + start_end_size + start;
		ushort len = end - start;
		str_lens[i] = len;
		str_total_len = std::max(str_total_len, end);
		max_len = std::max(max_len, len);
	}

	if(max_len == 0){
		free(str_lens);
		free(str_data_index);

		char* buffer = (char*)malloc(1 + sizeof(ushort));
		*buffer = (char) CPRS_SUCCESS;
		*(ushort*)(buffer + 1) = max_len;
		return (void*)buffer;
	}

	NumCompressor<ushort> index_compressor;
	char* index_cmp_data = JavaCompress_Number(str_lens, item_size, (_uint64)max_len, index_compressor);
	CprsErr index_cmp_err = (CprsErr)(*index_cmp_data);
	if (index_cmp_err != CPRS_SUCCESS){
		free(str_lens);
		free(str_data_index);

		return (void*)index_cmp_data;
	}

	uint index_cmp_data_size = *(uint*)(index_cmp_data + 9) + 1 + 8 + 4;

	char* str_cmp_data = JavaCompress_String(str_data_index, str_lens, item_size, str_total_len);
	CprsErr str_cmp_err = (CprsErr)(*str_cmp_data);
	
	free(str_lens);
	free(str_data_index);
	
	if (str_cmp_err != CPRS_SUCCESS){
		free(index_cmp_data);

		return (void*)str_cmp_data;
	}

	uint str_cmp_data_size = *(uint*)(str_cmp_data + 5) + 1 + 4 + 4;


	int cmp_header_size = 1 + 2 + 4 + 4;
	char* cmp_buffer = (char*)malloc(cmp_header_size + index_cmp_data_size + str_cmp_data_size);

	*cmp_buffer = (char)CPRS_SUCCESS;
	*(ushort*)(cmp_buffer + 1) = max_len;
	*(uint*)(cmp_buffer + 3) = index_cmp_data_size;
	*(uint*)(cmp_buffer + 7) = str_cmp_data_size;

	memcpy(cmp_buffer + cmp_header_size, index_cmp_data, index_cmp_data_size);
	memcpy(cmp_buffer + cmp_header_size + index_cmp_data_size, str_cmp_data, str_cmp_data_size);

	free(index_cmp_data);
	free(str_cmp_data);

	return cmp_buffer;
}

extern "C" void* JavaDecompress_IndexedString(void* cmp_data, unsigned int item_size){
	char* _cmp_data = (char*)cmp_data;

	CprsErr data_err = (CprsErr)*_cmp_data;
	if(data_err != CPRS_SUCCESS){
		printf("JavaDecompress_IndexedString data error: %d\n", data_err);
		return 0;
	}
	int cmp_header_size = 1 + 2 + 4 + 4;
	ushort max_len = *(ushort*)(_cmp_data + 1);

	if(max_len == 0){
		uint* buffer = (uint*)malloc(item_size * sizeof(uint));
		for(int i = 0; i < item_size; i++){
			buffer[i] = 0;
		}
		return buffer;
	}

	uint index_cmp_data_size = *(uint*)(_cmp_data + 3);
	uint str_cmp_data_size = *(uint*)(_cmp_data + 7);

	char* index_cmp_data = _cmp_data + cmp_header_size;
	NumCompressor<ushort> index_compressor;
	ushort* str_lens = (ushort*)JavaDecompress_Number(index_cmp_data, item_size, index_compressor);
	if(!str_lens){
		return 0;
	}

	char* str_cmp_data = _cmp_data + (cmp_header_size + index_cmp_data_size);
	char** str_index = (char**)malloc(item_size * sizeof(char*));
	char* str_data = JavaDecompress_String(str_cmp_data, str_index, str_lens, item_size);
	if(!str_data){
		free(str_lens);
		free(str_index);
		return 0;
	}

	int index_size = item_size * sizeof(uint) << 1;
	uint* str_start_ends = (uint*)malloc(index_size);
	uint str_total_len = 0;
	for(int i = 0; i < item_size; i ++){
		uint start = str_index[i] - str_data;
		uint end = start + str_lens[i];
		str_start_ends[i << 1] = start;
		str_start_ends[(i << 1) + 1] = end;
		str_total_len = std::max(str_total_len, end);
	}

	char* data_buffer = (char*)malloc(4 + index_size + str_total_len);
	*(int*)data_buffer = str_total_len;
	memcpy(data_buffer + 4, str_start_ends, index_size);
	memcpy(data_buffer + 4 +index_size, str_data, str_total_len);

	free(str_lens);
	free(str_index);
	free(str_data);
	free(str_start_ends);

	return data_buffer;
}


// data:
// | offset0 | offset1 | offset2 | offset3(str_total_len) | s0 | s1 | s2 |
// | <-                   index(int)                   -> |<- str_data ->|
//
// cmp_data:
// | err | index_cmp_data_size | str_cmp_data_size |     index_cmp_data     |         str_cmp_data         |
//    1            4                       4            index_cmp_data_size       str_cmp_data_size
// | <-           cmp_header                    -> |
//
extern "C" void* JavaCompress_IndexedString_v1(void* data, unsigned int item_size){
	uint* index = (uint*)data;
	uint  str_total_len = index[item_size];
	uint  index_len = (item_size + 1) * 4; // 4 bytes for int

	// Because bh compress cannot compress more than 65536 elements, so we ignore the first offset.
	// It is alright as it is always 0.
	BHASSERT(index[0] == 0, "The first offst should be 0");

	NumCompressor<uint> index_compressor;
	char* index_cmp_data = JavaCompress_Number(index + 1, item_size, (_uint64)str_total_len, index_compressor);

	CprsErr index_cmp_err = (CprsErr)(*index_cmp_data);
	if (index_cmp_err != CPRS_SUCCESS){
		return (void*)index_cmp_data;
	}

	char*   str_data = (char*)data + index_len;
	char**  str_data_pts = (char**)malloc(item_size * sizeof(char*));
	ushort* str_lens = (ushort*)malloc(item_size * sizeof(ushort));

	for(int i = 0; i < item_size; i ++){
		uint offset = index[i];
		uint end = index[i + 1];
		str_data_pts[i] = str_data + offset;
		str_lens[i] = (ushort)(end - offset);
	}

	char* str_cmp_data = JavaCompress_String(str_data_pts, str_lens, item_size, str_total_len);
	CprsErr str_cmp_err = (CprsErr)(*str_cmp_data);

	free(str_data_pts);
	free(str_lens);
	
	if (str_cmp_err != CPRS_SUCCESS){
		free(index_cmp_data);
		return (void*)str_cmp_data;
	}

	uint index_cmp_data_size = *(uint*)(index_cmp_data + 9) + 1 + 8 + 4;
	uint str_cmp_data_size = *(uint*)(str_cmp_data + 5) + 1 + 4 + 4;

	int cmp_header_size = 1 + 4 + 4;
	char* cmp_buffer = (char*)malloc(cmp_header_size + index_cmp_data_size + str_cmp_data_size);

	*cmp_buffer = (char)CPRS_SUCCESS;
	// *(ushort*)(cmp_buffer + 1) = max_len;
	*(uint*)(cmp_buffer + 1) = index_cmp_data_size;
	*(uint*)(cmp_buffer + 5) = str_cmp_data_size;

	memcpy(cmp_buffer + cmp_header_size, index_cmp_data, index_cmp_data_size);
	memcpy(cmp_buffer + cmp_header_size + index_cmp_data_size, str_cmp_data, str_cmp_data_size);

	free(index_cmp_data);
	free(str_cmp_data);

	return cmp_buffer;
}

extern "C" void* JavaDecompress_IndexedString_v1(void* cmp_data, unsigned int item_size){
	char* _cmp_data = (char*)cmp_data;

	CprsErr data_err = (CprsErr)*_cmp_data;
	if(data_err != CPRS_SUCCESS){
		printf("JavaDecompress_IndexedString data error: %d\n", data_err);
		return 0;
	}
	int cmp_header_size = 1 + 4 + 4;
	uint index_cmp_data_size = *(uint*)(_cmp_data + 1);
	uint str_cmp_data_size = *(uint*)(_cmp_data + 5);

	char* index_cmp_data = _cmp_data + cmp_header_size;
	NumCompressor<uint> index_compressor;
	
	// Keep in mind that the index doesn't contains the first offset(0).
	uint* index = (uint*)JavaDecompress_Number(index_cmp_data, item_size, index_compressor);
	if(!index){
		return 0;
	}

	ushort* str_lens = (ushort*)malloc(item_size * sizeof(ushort));
	for(int i = 0; i < item_size; i ++){
		uint offset = i == 0 ? 0 : index[i - 1];
		uint end = index[i];
		str_lens[i] = (ushort)(end - offset);
	}

	char* str_cmp_data = _cmp_data + (cmp_header_size + index_cmp_data_size);
	char** str_data_pts = (char**)malloc(item_size * sizeof(char*));
	char* str_data = JavaDecompress_String(str_cmp_data, str_data_pts, str_lens, item_size);
	if(!str_data){
		free(index);
		free(str_lens);
		free(str_data_pts);
		return 0;
	}

	uint  index_len = (item_size + 1) * 4; // 4 bytes for int
	uint  str_total_len = index[item_size - 1];
	char* data_buffer = (char*)malloc(index_len + str_total_len);

	((uint*)data_buffer)[0] = 0;
	memcpy(data_buffer + 4, index, index_len - 4);
	char* str_buffer = data_buffer + index_len;
	for(int i = 0; i < item_size; i ++){
		uint offset = i == 0 ? 0 : index[i - 1];
		memcpy(str_buffer + offset, str_data_pts[i], str_lens[i]);
	}

	free(index);
	free(str_lens);
	free(str_data_pts);
	free(str_data);

	return data_buffer;
}
