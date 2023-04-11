import hmac
import os
import logging

MESSAGE_LENGTH = 20
CHALLENGE = b'#CHALLENGE#'
WELCOME = b'#WELCOME#'
FAILURE = b'#FAILURE#'

class AuthenticationError(Exception):
    pass

def deliver_challenge(connection, authkey):
    if not isinstance(authkey, bytes):
        raise ValueError(
            "Authkey must be bytes, not {0!s}".format(type(authkey)))
    message = os.urandom(MESSAGE_LENGTH)
    assert len(message) == MESSAGE_LENGTH
    connection.sendall(CHALLENGE + message)
    digest = hmac.new(authkey, message, 'md5').digest()
    response = connection.recv(256)        # reject large message
    # digest, response
    if response == digest:
        connection.sendall(WELCOME)
    else:
        connection.sendall(FAILURE)
        raise AuthenticationError('digest received was wrong')

def answer_challenge(connection, authkey):
    if not isinstance(authkey, bytes):
        raise ValueError(
            "Authkey must be bytes, not {0!s}".format(type(authkey)))
    message = connection.recv(256)         # reject large message
    assert message[:len(CHALLENGE)] == CHALLENGE, 'message = %r' % message
    message = message[len(CHALLENGE):]
    digest = hmac.new(authkey, message, 'md5').digest()
    connection.sendall(digest)
    response = connection.recv(len(WELCOME))        # reject large message
    if response != WELCOME:
        raise AuthenticationError('digest sent was rejected')



def get_key(authfile) -> bytes:
    """Read the secret key from the given file, used in the HMAC auth, return as bytes"""
    with open(authfile,'r',encoding='utf8') as fii:
        for line in fii.readlines():
            l=line.strip()
            if len(l) == 0:
                continue
            if l[0] == "#":
                continue
            key = l
            break
    return key.encode('utf8')
