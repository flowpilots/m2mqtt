/*
M2Mqtt Project - MQTT Client Library for .Net and GnatMQ MQTT Broker for .NET
Copyright (c) 2014, Paolo Patierno, All rights reserved.

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 3.0 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library.
*/

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
[assembly: AssemblyCopyright("Copyright © Paolo Patierno 2014")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]

// Version information for an assembly consists of the following four values:
//
//      Major Version
//      Minor Version 
//      Build Number
//      Revision
//
[assembly: AssemblyVersion("3.4.0.0")]
// to avoid compilation error (AssemblyFileVersionAttribute doesn't exist) under .Net CF 3.5
#if !WindowsCE
[assembly: AssemblyFileVersion("3.4.0.0")]
#endif