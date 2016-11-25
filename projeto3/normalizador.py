#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import re
import htmlentitydefs
from unicodedata import normalize
from doctest import testmod

# http://effbot.org/zone/re-sub.htm#unescape-html
def unescape(text):
    def fixup(m):
        text = m.group(0)
        if text[:2] == "&#":
            # character reference
            try:
                if text[:3] == "&#x":
                    return unichr(int(text[3:-1], 16))
                else:
                    return unichr(int(text[2:-1]))
            except ValueError:
                pass
        else:
            # named entity
            try:
                text = unichr(htmlentitydefs.name2codepoint[text[1:-1]])
            except KeyError:
                pass
        return text # leave as is
    return re.sub("&#?\w+;", fixup, text)

class Normalizador(object):
    def __init__(self):
        self.symbols = re.compile(r'\W+')

    def normalizaAcentos(self, texto):
        return self.normalizaTextoComSimbolos(texto, lowcase=False)

    def normalizaTextoHTMLComSimbolos(self, texto):
        texto_normalizado = texto
        try:
            # Tentativa 1: texto ASCII ou ASCII-like
            texto_normalizado = unescape(unicode(texto_normalizado))
            texto_normalizado = normalize('NFKD', texto_normalizado).encode('ASCII','ignore')
        except Exception as e:
            try:
                # Tentativa 2: UTF-8
                texto_normalizado = unescape(texto_normalizado.decode('utf-8'))
                texto_normalizado = normalize('NFKD', texto_normalizado).encode('ASCII','ignore')
            except Exception as e2:
                # Tentativa 3: ISO-8859-1 (a.k.a. latin-1)
                texto_normalizado = unescape(texto_normalizado.decode('iso-8859-1'))
                texto_normalizado = normalize('NFKD', texto_normalizado).encode('ASCII','ignore')

        return texto_normalizado

    def normalizaTextoComSimbolos(self, texto, lowcase=True):
        texto_normalizado = texto
        try:
            # Tentativa 1: texto ASCII ou ASCII-like
            texto_normalizado = normalize('NFKD', unicode(texto_normalizado)).encode('ASCII','ignore').lower()
        except Exception as e:
            try:
                # Tentativa 2: UTF-8
                texto_normalizado = normalize('NFKD', texto_normalizado.decode('utf-8')).encode('ASCII','ignore').lower()
            except Exception as e2:
                # Tentativa 3: ISO-8859-1 (a.k.a. latin-1)
                texto_normalizado = normalize('NFKD', texto_normalizado.decode('iso-8859-1')).encode('ASCII','ignore').lower()

        if lowcase: return texto_normalizado.lower()
        else: return texto_normalizado

    def converterSimbolosHTML(self, texto):
        return unescape(unicode(texto))
    def normalizaTexto(self, texto):
        return self.symbols.sub(' ', self.normalizaTextoComSimbolos(texto))
    def normalizaTextoHTML(self, texto):
        return self.symbols.sub(' ', self.normalizaTextoHTMLComSimbolos(texto))

if __name__ == "__main__":

    normalizador = Normalizador()
    texto = "Jogo de Super Banho Sâmia 5 Peças - Casa &amp; Conforto - ō"
    print texto
    print normalizador.normalizaTextoComSimbolos(texto)
    texto = "Jogo de Super Banho S&acirc;mia 5 Peças - Casa &amp; Conforto - ō"
    print texto
    print normalizador.normalizaTextoHTMLComSimbolos(texto)

