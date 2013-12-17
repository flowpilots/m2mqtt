using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

// General Information about an assembly is controlled through the following 
// set of attributes. Change these attribute values to modify the information
// associated with an assembly.
[assembly: AssemblyTitle("M2Mqtt")]
[assembly: AssemblyDescription("MQTT Client Library for M2M communication")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("Paolo Patierno")]
[assembly: AssemblyProduct("M2Mqtt")]
[assembly: AssemblyCopyright("Copyright © Paolo Patierno 2013")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]

// Version information for an assembly consists of the following four values:
//
//      Major Version
//      Minor Version 
//      Build Number
//      Revision
//
[assembly: AssemblyVersion("2.3.1.0")]
// to avoid compilation error (AssemblyFileVersionAttribute doesn't exist) under .Net CF 3.5
#if !WindowsCE
[assembly: AssemblyFileVersion("2.3.1.0")]
#endif