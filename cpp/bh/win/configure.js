/******************************************************************
 *
 *  configure.js
 *
 *  Script to configure Infobright 3.1 for Windows instance
 *
 ******************************************************************/

default_port = "3306";

ForReading = 1;
ForWriting = 2;
ForAppending = 8;

var fso = new ActiveXObject("Scripting.FileSystemObject");
var current_dir = fso.GetFolder(".").path;

current_dir = replaceAll( current_dir, "\\", "/");

/*******************************************************************
 * Process configuration template and create config file my-ib.ini
 *******************************************************************/
var configTemplate = current_dir + "/my-ib.in_";
if (! fso.FileExists( configTemplate)) {
    WScript.Echo("Error: missing my-ib.in_ file");
    WScript.Quit(-1);
}
var src = fso.OpenTextFile( configTemplate, ForReading);
var text = src.ReadAll();
src = null;
text = replaceAll (text, "{BASE_DIR}", current_dir);
text = replaceAll (text, "{PORT}", default_port);
var configFile = current_dir + "/my-ib.ini";
var target = fso.OpenTextFile( configFile, ForWriting, true);
target.Write( text);
target = null;
//fso.DeleteFile( configTemplate, true);

/*********************************************************************
 * Create cache directory
 *********************************************************************/
var cacheDir = current_dir+"/cache";
if (! fso.FolderExists( cacheDir)) {
    fso.CreateFolder( cacheDir);
}

/*******************************************************************
 * Prepare Brighthouse configuration file data/brighthouse.ini
 *******************************************************************/
var dataDir = current_dir+"/data";
if (! fso.FolderExists( dataDir)) {
    WScript.Echo("Error: missing data directory");
    WScript.Quit(-1);
}
var brighthouseTemplate = dataDir + "/brighthouse.in_";
src = fso.OpenTextFile( brighthouseTemplate, ForReading);
text = src.ReadAll();
src = null;
text = replaceAll (text, "{BASE_DIR}", current_dir);
var brighthouseConfig = dataDir + "/brighthouse.ini";
target = fso.OpenTextFile( brighthouseConfig, ForWriting, true);
target.Write( text);
target = null;
//fso.DeleteFile( brighthouseTemplate);

/*******************************************************************
 * Prepare batch files
 *******************************************************************/
var starter = current_dir + "/start-ib.bat";
target = fso.OpenTextFile( starter, ForWriting, true);
target.WriteLine( "@echo OFF");
target.WriteLine( "echo Running ICE 3.1 for Windows");
target.WriteLine( "echo --- use stop-ib.bat to shutdown");
target.WriteLine( "echo --- shutdown can take a while");
target.WriteLine( "\"" + current_dir+"/bin/mysqld-nt.exe\" --version");
target.WriteLine( "\"" + current_dir+"/bin/mysqld-nt.exe\" --defaults-file=\"" + configFile + "\"");
target = null;

var stopper = current_dir + "/stop-ib.bat";
target = fso.OpenTextFile( stopper, ForWriting, true);
target.WriteLine( "\"" + current_dir+"/bin/mysqladmin.exe\" --defaults-file=\"" + configFile + "\" --user=root shutdown");
target = null;

/*******************************************************************
 * Unlock executables
 *******************************************************************/
fso.MoveFile( current_dir+"/bin/mysqld-nt.ex_",
              current_dir+"/bin/mysqld-nt.exe");

fso.MoveFile( current_dir+"/bin/mysqld.ex_",
              current_dir+"/bin/mysqld.exe");

fso.MoveFile( current_dir+"/bin/bhloader.ex_",
              current_dir+"/bin/bhloader.exe");

WScript.Echo("ICE 3.1 Win32 is configured!\n Use start-ib.bat and stop-ib.bat to start and stop the server.");
 
/*******************************************************************
 * Utilities
 *******************************************************************/

/***
 * replaceAll replaces all entries of a string _from_ in a string _str_
 * by a string _to_
 ***/
function replaceAll( str, from, to){
    while (str.indexOf(from) >= 0) {
        str = str.replace( from, to);
    }
    return str;
}

