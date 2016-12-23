
#ifndef __IB_LICENSE_H__
#define __IB_LICENSE_H__

#include <string>
#include "common/stdafx.h"

extern const char* EXPIRATION_MESSAGE;

#define IB_LICENSE_PREFIX      "iblicense-"
#define IB_LICENSE_TIMESTAMP   "iblicense.dat"

struct _LicenseTimeStamp
{
    time_t start_date;
    time_t expiry_date;
    time_t last_timestamp;
    _int64 licenseID;
};

typedef _LicenseTimeStamp LicenseTimeStamp;

class IBLicense
{
private:
    static IBLicense* self;

public:
    static IBLicense* GetLicense();
    int Load();

    // key file path is externally configurable, othewise it will use product dir
    void LoadLicenseKeyFile();
    int LoadLicenseTimeStampFile();
    int UpdateLicenseTimeStamp();
    int SaveLicenseTimeStamp();

    std::string GetVersion() const { return version; }
    time_t GetExpire() const { return expire; }

    int IsExpired(bool update_timestamp);
    int IsExpiredDML(bool update_timestamp = false);
	bool IsTimeExpired();

    void SetProductDir(std::string dirpath) { productdir = dirpath; }
    void SetDataDir(std::string dirpath) { datadir = dirpath; }
    int SetKeyFile(std::string filepath);

	typedef enum { PRODUCT_IEE, PRODUCT_DLP } ProductType;
	bool IsProduct( ProductType prod );
private:
    enum { MAX_LICENSE_FILE_SIZE = 4096 };
    char data [MAX_LICENSE_FILE_SIZE];

    // Licesense manager uses two directories: product dir where it stores license key file and timestamp file.
    // datadir is the the directory to check whether it is fresh or used. When timestamp file is fresh, data directory 
    // requires to be fresh.
    std::string productdir, datadir;
    
    // Licesense manager uses two files, license key file which are created and send from vendor
    // timestamp file contains initially all zero and stays in the product dir. It will be updated by every time when
    // teh product will be used.
    std::string keyfile, timestampfile;

    // key file info
    static const char* VERSION_NAME;
    static const char* EXPIRE_NAME;
    static const char* START_NAME;
    static const char* LICENSE_ID;

    std::string version;
    time_t      expire;
    time_t      start;
    _int64      licenseID;
    LicenseTimeStamp lic_timestamp;

    std::string Extract( const char* name);
};

#endif /* __IB_LICENSE_H__ */

