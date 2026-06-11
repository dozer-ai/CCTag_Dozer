/*
 * Copyright 2016, Simula Research Laboratory
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
#include <iostream>

#include "tag_threads.h"
#include "tag.h"
#include "frame.h"

namespace cctag
{

using namespace std;

TagThread::TagThread( TagThreads* creator, TagPipe* pipe, int layer )
    : std::thread( )           // default-construct: do NOT launch the worker yet
    , _creator( creator )
    , _pipe( pipe )
    , _my_layer( layer )
{
    /* The base std::thread subobject is constructed before these members, so
     * launching the worker in the base initializer (the upstream form) races:
     * call() could dereference _creator/_pipe before they are assigned. Start
     * the thread only now that all members are initialized. */
    std::thread::operator=( std::thread( &TagThread::call, this ) );
}

void TagThread::call( void )
{
    _creator->startWait( );

    while( true ) {
        _creator->frameReadyWait( );

        /* A frameReady post during shutdown is the signal to exit. Check
         * before touching the pipe or the frameDone semaphore so we fall out
         * of the loop instead of using freed state. */
        if( _creator->isStopping( ) ) {
            break;
        }

        _pipe->handleframe( _my_layer );

        _creator->frameDonePost( );
    }
}

TagThreads::TagThreads( )
    : _pipe( nullptr )
    , _layers( 0 )
    , _stop( false )
    , _start( 0 )
    , _frameReady( 0 )
    , _frameDone( 0 )
{ }

TagThreads::~TagThreads( )
{
    _stop = true;

    /* Wake every worker that may be parked on a semaphore so it observes
     * _stop and returns from call(). Without this the threads would still be
     * blocked in wait() when the semaphores below them destruct, locking a
     * dead mutex (EINVAL -> std::terminate) on teardown/restart. */
    _start.post( _layers );
    _frameReady.post( _layers );

    for( TagThread* t : _threadList ) {
        if( t->joinable( ) ) {
            t->join( );
        }
        delete t;
    }
    _threadList.clear( );
}

void TagThreads::init( TagPipe* pipe, int layers )
{
    _pipe   = pipe;
    _layers = layers;

    for( int i=0; i<_layers; i++ ) {
        _threadList.push_back( new TagThread( this, _pipe, i ) );
    }

    startPost( );
}

void TagThreads::oneRound( )
{
    frameReadyPost( );
    frameDoneWait( );
}

void TagThreads::startWait( )      { _start.wait( 1 );  }
void TagThreads::startPost( )      { _start.post( _layers ); }
void TagThreads::frameReadyWait( ) { _frameReady.wait( 1 );  }
void TagThreads::frameReadyPost( ) { _frameReady.post( _layers ); }
void TagThreads::frameDoneWait( )  { _frameDone.wait( _layers );  }
void TagThreads::frameDonePost( )  { _frameDone.post( 1 ); }

/*************************************************************
 * TagSemaphore
 *************************************************************/

void TagSemaphore::wait( int n )
{
    std::unique_lock<std::mutex> sema_lock( _sema_mx );
    while( _sema_val - n < 0 )
    {
        _sema_cond.wait( sema_lock );
    }
    _sema_val -= n;
    sema_lock.unlock();
}

void TagSemaphore::post( int n )
{
    std::unique_lock<std::mutex> sema_lock( _sema_mx );
    _sema_val += n;
    _sema_cond.notify_all();
    sema_lock.unlock();
}

}; // namespace cctag

