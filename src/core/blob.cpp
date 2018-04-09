//
// Copyright (C) 2004-2008 Maciej Sobczak, Stephen Hutton
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//

#define SOCI_SOURCE
#include "blob.h"
#include "session.h"

#include <cstddef>
#include <fstream> 

using namespace soci;

blob::blob(session & s)
	:backEnd_(0)
{
    backEnd_ = s.make_blob_backend();
}

blob::~blob()
{
	if( backEnd_ ) { delete backEnd_; backEnd_ = 0; }
}

std::size_t blob::get_len() const
{
    return backEnd_->get_len();
}

std::size_t blob::read(std::size_t offset, char *buf, std::size_t toRead) const
{
    return backEnd_->read(offset, buf, toRead);
}

std::size_t blob::read(char *buf, std::size_t toRead) const
{
    return backEnd_->read(backEnd_->get_start_index(), buf, toRead);
}

std::size_t blob::write(char const * buf, std::size_t toWrite)
{
    return backEnd_->write(backEnd_->get_start_index(), buf, toWrite);
}

std::size_t blob::write(
    std::size_t offset, char const * buf, std::size_t toWrite)
{
    return backEnd_->write(offset, buf, toWrite);
}

std::size_t blob::append(char const * buf, std::size_t toWrite)
{
    return backEnd_->append(buf, toWrite);
}

void blob::trim(std::size_t newLen)
{
	backEnd_->trim(newLen);
}

void blob::clear()
{
	backEnd_->trim(0);
}

soci::blob & blob::operator =( const soci::blob & src)
{
	size_t len = src.get_len();
	char * buf = new char[len];
	src.read(buf,len);
	this->write(buf,len);
	delete [] buf;
	return *this;
}

std::size_t blob::writeToFile(char const * strFilePath) const
{
	std::size_t iLen = get_len();
	char * buf = new char[iLen];
	read(buf, iLen);

	std::ofstream outfile (strFilePath,std::ofstream::binary);
	outfile.write(buf, iLen);
	outfile.close();

	delete [] buf;

	return iLen;
}

std::size_t blob::readFromFile(char const * strFilePath)
{
	std::ifstream infile (strFilePath,std::ifstream::binary);

	// get size of file
	infile.seekg (0,infile.end);
	std::size_t iLen = infile.tellg();
	infile.seekg (0);
	
	char * buf = new char[iLen];
	infile.read(buf,iLen);

	write(buf,iLen);
	
	delete [] buf;

	infile.close();

	return iLen;
}