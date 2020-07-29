---
title: Erasure Coding in Ozone 
summary: Crypto key management to handle GDPR "right to be forgotten" feature
date: 2020-06-30
jira: HDDS-3816
status: draft
author: Uma Maheswara Rao Gangumalla, Marton Elek, Stephen O'Donnell 
---

# Abstract

 Support Erasure Coding for read and write pipeline of Ozone.
  
# Status

 The design doc describe two main method to implement EC:
 
  * Container level, async Erasure Coding, to encode closed containers in the background
  * Block level, striped Erasure Coding
 
 Second option can work only with new, dedicated write-path. Details of possible implementation will be included in the next version.
 
# Link

 https://issues.apache.org/jira/secure/attachment/13006245/Erasure%20Coding%20in%20Apache%20Hadoop%20Ozone.pdf

