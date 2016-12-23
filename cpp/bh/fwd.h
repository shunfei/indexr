#ifndef FWD_H_INCLUDED
#define FWD_H_INCLUDED 1

#include "edition/local_fwd.h"
#include <vector>

typedef boost::shared_ptr<RCTable> RCTablePtr;

class ConnectionInfo;
class IOParameters;
class RCDataType;
class AttributeTypeInfo;
typedef AttributeTypeInfo ATI;

typedef boost::shared_ptr<RCTable> RCTablePtr;

class TableLock;
typedef boost::shared_ptr<TableLock>    TableLockPtr;
typedef boost::weak_ptr<TableLock>              TableLockWeakPtr;

class JustATable;
typedef boost::shared_ptr<JustATable>   JustATablePtr;
typedef boost::weak_ptr<JustATable>             JustATableWeakPtr;

class Filter;
typedef boost::shared_ptr<Filter> FilterPtr;

class TempTable;
typedef boost::shared_ptr<TempTable>    TempTablePtr;
typedef boost::weak_ptr<TempTable>              TempTableWeakPtr;

class AttrPack;
typedef boost::shared_ptr<AttrPack> AttrPackPtr;

class DataExporter;
typedef std::auto_ptr<DataExporter> DataExporterAutoPtr;

class DataFormat;
typedef boost::shared_ptr<DataFormat> DataFormatPtr;

class ValueParser;
typedef std::auto_ptr<ValueParser> ValueParserAutoPtr;

class ParsingStrategy;
typedef boost::shared_ptr<ParsingStrategy> ParsingStrategyAutoPtr;

typedef std::vector<boost::shared_ptr<RCDataType> > NewRecord;

#endif /* #ifndef FWD_H_INCLUDED */

