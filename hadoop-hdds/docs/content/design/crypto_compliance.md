---
title: Crypto compliance in Apache Ozone 
summary: Different legislations have different rules for cryptography usage in certain environments, therefore we define the separation of our crypto usage to provide proper pluggability of a compliant crypto environment.
date: 2024-02-01
jira: HDDS-10234
status: draft
author: Istvan Fajth 
---
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Crypto compliance in Apache Ozone

---
### Separation of the used cryptography implementation within Ozone, steps towards a strict Cryptographic boundary.

## Problem statement

In some jurisdictions there is legislation that defines how and which cryptographic libraries and algorithms can be
used in regulated environments, therefore Apache Ozone should have a way to configure and restrict cryptographic
operations to conform and enforce the legislation and to allow a feasible way to certify and run it in these regulated
environments.


## Goals

The main goal of this design document to lay down the foundations of how Apache Ozone can provide a way for anyone who
is interested to make Apache Ozone compliant with Cryptography related legislation in a given jurisdiction.  
The best way of doing so is to define the cryptographic boundary of Apache Ozone, in a way that all parts of the
codebase is replaceable with a compliant implementation whenever it is needed, and all the configurable options that
are defined for certain cryptography related functionalities can easily be overridden, and checked against a set of
rules to ensure it is forbidden to configure any non-compliant function/algorithm/parameter.


## Non goals

It is not a goal to define any sets of rules or other policies which aims to conform with any particular legislation,
or to make an attempt to certify Apache Ozone in any jurisdictions.  
In the current design, we do not plan to have support to go from unrestricted to compliant on the same cluster, because
this move can affect a multitude of things, however later on if there is a need to address this move in place, we
can re-iterate this thought. Such a move certainly requires more integration points, and it may be fairly complex,
if for example we need to re-generate all certificates and update how they are stored, or if all block checksums have
to be recalculated in order to comply with a new legislative environment.


## Proposed design

The general idea to solve this question is to enable the restriction of any cryptographic 
functions/algorithms/parameters via configuration.  
This way it is possible to define a configuration file, that is loaded from the classpath and defines the allowed
configuration options via a whitelist parameter for given configuration options, if needed a new legislation relative
default can also be considered.

Unfortunately though this is not enough, as some of our code (mainly for the internal PKI system which is inevitable
for gRPC SSL/TLS) is using BouncyCastle directly not via the JCE Provider which may or may not be compliant with a 
given legislation.  
In order to overcome this situation, we think about introducing a Service Provider Interface, via which the current
functions used from BouncyCastle can be defined separately from the main code, and later can be replaced via a
different implementation by changing the classpath, or via configuring the SPI implementation directly.  
This SPI may be extended with other functions besides BouncyCastle calls, as most of our cryptography is going through
the Java Cryptographic Extensions, for which a compliant provider may be set up. In case this provider does not support
all functions we use, we may add other compliant solutions external to JCE in which case this SPI can also serve as
the layer of abstraction between Ozone code and Cryptography functions in the external source and/or the JCE based
default implementation.


### Proposal for configuration related changes

We propose the following changes to configuration in general:
- introduce a main configuration option (`ozone.security.crypto.compliance.mode`) that defines a name for the
legislation module to be used with a default value of `unrestricted` as a free form String option. 
- introduce a new configuration option tag `CRYPTO_COMPLIANCE` and tag all existing configuration options for 
cryptographic functions/algorithms/parameters.
- introduce new configuration options for any currently used cryptographic functions/algorithms/parameters that are
not configurable yet, and tag them with the `CRYPTO_COMPLICANCE` and other relevant tags.

Note that anything that is configurable via the java.security file, or the Security class, we consider configurable,
such as selecting a JCE provided default implementation is something we may consider configurable.
This way we can reduce the extent of the initial problem at hand, and defer extending configuration extensively for all
different parameters that can be set to the configured Providers in JCE.

With that once we have all the configuration options defined, we need to ensure that all the cryptography related
configuration options are checked properly against optionally defined whitelists in case the 
`ozone.security.crypto.compliance.mode` is set to anything else then `unrestricted` (the default).

The proposed way of checking these options is to override methods of our OzoneConfiguration, and ConfigurationSource,
so that for any configuration option being read - in case the value of `ozone.security.crypto.compliance.mode` is not
`unrestricted` and the option is tagged with the `CRYPTO_COMLIANCE` tag - the implementation checks whether there is 
a whitelist containing a comma separated list of allowed values defined for the given configuration option, and the 
configured whitelist contains the value set, if it does not then getting the configuration value should throw an
exception, or otherwise signal an error that halts further execution. This way the software ensures that if there is an
invalid configuration set for the given compliance mode, it refuses to operate until it becomes compliant.

The whitelist is assumed to be defined under the same configuration option key as the configuration option evaluated,
suffixed with the value of `ozone.security.crypto.compliance.mode` and the string `whitelist` using the regular dot
separator in between words in the name of the configuration option.  
As an example: for config option `crypto.function` the whitelist for compliance mode `neverland` is assumed to be
configured as `crypto.function.neverland.whitelist`.   

As in order to apply a configuration change, the configuration value has to be read from the Configuration object,
we can allow any value to be set, and just focus these checks on any property reads so the actual code can not read
a value that is not whitelisted without running into a failure.  
In case of `unrestricted` compliance mode none of the whitelists should be searched or checked, and any otherwise valid
property value should be allowed to be used as it is currently, to ensure the least amount of overhead for this case.

A whitelist may contain only the `*` (wildcard) character, which is equivalent with an undefined whitelist, and allows
any value to be set which is otherwise allowed to be set in unrestricted mode.  
If a whitelist contains other elements together with the `*` (wildcard) character, that is an ambiguous configuration,
which should produce a WARN level log message, and otherwise behave as if the whitelist would be undefined, or as if
the whitelist would only contain the `*` (wildcard) character.

These additional configuration options should be automatically loaded from the classpath if they are added in a file
named after the crypto compliance mode name as `ozone-crypto-<modeName>.xml`.
We may consider adding the possibility to define other sources via configuration, but as the load from a specific path
approach is feasible, this may be an extra addition later on if there is a need, and it is not part of the initial
scope.

### Proposal for the SPI enabling us to optionally replace BouncyCastle

Our dependency on BouncyCastle as a JCE Provider is problematic in at least one jurisdiction, as it is not certified to
comply the relevant US legislation. With that in order to ensure compliance is possible in all jurisdictions we need
to ensure that any call to BouncyCastle can easily be replaced to a compliant call.

So to achieve this, the easiest way is to define an interface behind which these calls can be hidden in the default 
`unrestricted` mode, and for any other compliance mode it should be possible to define and load a compliant 
implementation.

That said we propose the following changes to the codebase:
- Define a Service Provider Interface that can be discovered and loaded via ServiceLoader (META-INF/services) or via 
a configuration option, which then provides implementation for the required crypto functions in a compliant way.
  - The SPI implementation should provide a method that decides if the actual implementation should be loaded based on
  the value of `ozone.security.crypto.compliance.mode`, so that the ServiceLoader can select which implementation to
  actually load based on the compliance mode, without configuring specific classes. It selects the first conformant
  implementation it finds.
  - The SPI should then be able to lazily initialize and provide (possibly multiple) objects that implements interfaces
  defined for different use cases in our codebase. (At this point it is early to tell if it is feasible to have just
  one interface, or we may desire to use more than one smaller but specific interface for different usages.)
- Create a module called hdds-unrestricted-crypto that contains the implementation of the default `unrestricted`
compliance mode with the current BouncyCastle based implementation, and hook this in into the release build.
- As we currently use and configure BouncyCastle as the first Provider in JCE, and as we do this programmatically,
we need to ensure that an implementor of the SPI has the chance to initialize Java Security on its own, and we need to
move the code that sets BouncyCastle to be the first JCE provider to the implementation of the `unrestricted` module,
and then ensure that it is called at the same time as it is now, but via the SPI.  
Note that this initialization call may be undesirable, and we may need to allow disabling the initialization call for
use cases where the implementor or the administrator would like to have guarantees on JCE configuration, and does not
allow any programmatic changes to the java security configuration, via a SecurityManager for example.

Note that in this proposal we leave some of the usages of JCE untouched - with that irreplaceable - in the codebase, and
although this is not intuitive, but it is justifiable, as JCE provides the possibility to plug in any compliant
solution on its own. It is debatable whether we should or should not move these calls to our crypto module interface
definitions.  
The only real advantage of doing so would be the promise of having a well-defined cryptographic boundary that a crypto
compliance mode plugin provider can deal with at once instead of having to deal with JCE separately.
This we see as an additional complexity at this point, and we do not see the justification for it, as it would induce
an additional burden on the community to keep this in mind, and use JCE in any new code only by extending the module
in a backward compatible way considering possible implementors that may already exist at that point in time, as than a
change in these interfaces may break the compatibility with these additional implementations out there.
Therefore, we decided to skip this step as of now, and iterate back on this part later on if we ever see a need
to add another separation between our code and JCE to overcome a problem with compliance mode implementations.


## To summarize

After the proposed changes, crypto compliance in Apache Ozone will rely on configurable whitelists via which it is
possible to restrict the used cryptographic functions/algorithms/parameters, and it will be possible to replace the
implementation of functions that directly rely on BouncyCastle or if needed than later cryptography at all.  
Our configuration will be extended to allow configuring all the code paths where we use cryptography in any way, while
our usage of cryptography via the Java Cryptography Extensions (JCE) can be configured via the java.security file,
and this configuration may be overridden programmatically from a cryptography module's implementation.

The main configuration option is `ozone.security.crypto.compliance.mode` which contains the name of the crypto
compliance module being used. Based on this configuration we load two things, the relevant configuration options from
an additional configuration file (`ozone-crypto-<modeName>.xml`), and the first implementation of a Service Provider
Interface that provides the implementation of some functions for the compliance mode set.

These facilities combined with the versatility and configurability of the Java CryptoGraphic Extensions via the 
java.security file ensures that all aspects of crypto usage in Apache Ozone is configurable or replaceable to make
Apache Ozone compliant with the legislation of a given jurisdiction.
