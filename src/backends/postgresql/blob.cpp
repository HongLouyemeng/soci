//
// Copyright (C) 2004-2008 Maciej Sobczak, Stephen Hutton
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//

#define SOCI_POSTGRESQL_SOURCE
#include "soci-postgresql.h"
#include <libpq/libpq-fs.h> // libpq
#include <cctype>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <sstream>
#include <algorithm>

#ifdef SOCI_POSTGRESQL_NOPARAMS
#ifndef SOCI_POSTGRESQL_NOBINDBYNAME
#define SOCI_POSTGRESQL_NOBINDBYNAME
#endif // SOCI_POSTGRESQL_NOBINDBYNAME
#endif // SOCI_POSTGRESQL_NOPARAMS

#ifdef _MSC_VER
#pragma warning(disable:4355)
#endif

using namespace soci;
using namespace soci::details;


postgresql_blob_backend::postgresql_blob_backend(
    postgresql_session_backend & session)
    : session_(session), buf_(0), len_(0)
{
    // nothing to do here, the descriptor is open in the postFetch
    // method of the Into element
}

postgresql_blob_backend::~postgresql_blob_backend()
{
	if (buf_)
    {
        delete [] buf_;
        buf_ = 0;
        len_ = 0;
    }
}

std::size_t postgresql_blob_backend::get_len()
{
    return len_;
}

std::size_t postgresql_blob_backend::read(
    std::size_t offset, char * buf, std::size_t toRead)
{
    size_t r = toRead;

    // make sure that we don't try to read
    // past the end of the data
    if (r > len_ - offset)
    {
        r = len_ - offset;
    }

    memcpy(buf, buf_ + offset, r);

    return r;
}

std::size_t postgresql_blob_backend::write(
    std::size_t offset, char const * buf, std::size_t toWrite)
{
	 const char* oldBuf = buf_;
    std::size_t oldLen = len_;
    len_ = (offset + toWrite);

	if( offset > oldLen ) 
		throw soci::soci_error("offset greater than old length");

    buf_ = new char[len_];

    if (oldBuf)
    {
        // we need to copy both old and new buffers
        // it is possible that the new does not
        // completely cover the old
        memcpy(buf_, oldBuf, offset);
        delete [] oldBuf;
    }
    memcpy(buf_ + offset, buf, toWrite);

    return len_;
}
    

std::size_t postgresql_blob_backend::append(
    char const * buf, std::size_t toWrite)
{
    const char* oldBuf = buf_;

    buf_ = new char[len_ + toWrite];

    memcpy(buf_, oldBuf, len_);

    memcpy(buf_ + len_, buf, toWrite);

    delete [] oldBuf;

    len_ += toWrite;

    return len_;
}

void postgresql_blob_backend::trim(std::size_t  newLen )
{
    const char* oldBuf = buf_;
    len_ = newLen;

    buf_ = new char[len_];
	memcpy(buf_, oldBuf, len_);

    delete [] oldBuf;
}

const char* postgresql_blob_backend::data() const
{
    return buf_;
}

std::size_t postgresql_blob_backend::set_data(char const *buf, std::size_t toWrite)
{
    if (buf_)
    {
        delete [] buf_;
        buf_ = 0;
        len_ = 0;
    }
    return write(0, buf, toWrite);
}
