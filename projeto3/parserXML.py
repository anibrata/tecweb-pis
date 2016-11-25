# -*- coding: utf-8 -*- 

from lxml import etree, objectify
from normalizador import Normalizador
import gc
import os
import sys
import codecs
import json
import re
from collections import defaultdict
try:
    import simplejson as json
except ImportError:
    import json

class Tag(object):
    ''' classe para representar uma tag XML '''
    def __init__(self, nome, texto, atribs, nome_pai):
        self.nome = nome.strip().lower()
        self.nome_pai = nome_pai.strip().lower()
        self.texto = texto.strip()
        self.atribs = atribs

    def __repr__(self):
        return "[%s]: [%s], [%s], [%s]" % (self.nome, self.texto, self.atribs, self.nome_pai)

class XMLConverter(object):
    def __init__(self, xml, tag):
        self.xmlFile = xml
        self.tagProduto = tag

    def limpaTag(self, tag):
        return tag[tag.find('}')+1:]

    def atualizarDict(self, d, v):
        key = v.keys()[0]
        if key in d:
            if type(d[key]) == list:
                d[key].append(v[key])
            else:
                d[key] = [d[key], v[key]]
        else:
            d.update(v)

    def buildDict(self, element):
        tag = self.limpaTag(element.tag)
        # tag = str(element.xpath('local-name()'))

        if len(element.getchildren()) == 0:
            text = ''
            if element.text:
                text = element.text.strip()
            if element.attrib:
                item = {}
                item['neemu_atributos'] = dict(element.attrib)
                item['texto'] = text
                return {tag: item}
            else:
                return {tag: text}

        else:
            item = {}
            for child in element.getchildren():
                value = self.buildDict(child)
                self.atualizarDict(item, value)
                
            if element.attrib:
                item['neemu_atributos'] = dict(element.attrib)
            return {tag: item}

    def convertXMLToDict(self):
        ''' Converte as informações do produto de XML para Dict '''
        fin = open(self.xmlFile, 'r')
        for _, el1 in etree.iterparse(fin, tag=self.tagProduto):
            dtemp = {}
            for e in el1.getchildren():
                d = self.buildDict(e)
                self.atualizarDict(dtemp, d)
                e.clear()
                d.clear()
            if el1.attrib:
                dtemp['neemu_atributos'] = dict(el1.attrib)
            yield dtemp
            el1.clear()
        fin.close()

    def convertXML(self):
        ''' Converter todas as tags em uma instancia da classe Tag '''
        fin = open(self.xmlFile, 'r')
        for event, el1 in etree.iterparse(fin, tag=self.tagProduto):
            dtemp = defaultdict(list)
            for event, el2 in etree.iterwalk(el1):
                if el2.text is None: el2.text = ""
                dtemp[el2.tag.strip().lower()].append(
                        Tag(el2.tag, el2.text, dict(el2.attrib), el2.getparent().tag))
            el1.clear()
            while el1.getprevious() is not None:
                del el1.getparent()[0]
            yield dtemp
            dtemp = {}
 #           dtemp = None
#            gc.collect()
        fin.close()

    # Este metodo extrai o arquivo XML de forma encapsulada, mantendo a hierarquia interna dos itens.
    # O metodo carrega a base toda, pode ser lento/problematico com XMLs muito grandes.
    def convertXMLObj(self):
        # Abre o arquivo XML, carrega no objeto root
        root = objectify.fromstring(open(self.xmlFile, 'r').read())

        # Retorna os objetos encapsulados, extraidos do XML.
        for entry in root[self.tagProduto]:
            yield entry
            entry = None
        root = None
    #    gc.collect()


def addStringTermsToVec(s, phrase):
    phrase = norm.normalizaTextoComSimbolos(regex.sub(' ', phrase))
    for t in phrase.split():
        s.append(t)

def getAllTerms(d):
    js = json.dumps(d)
    vecOfTerms = []
    addStringTermsToVec(vecOfTerms, js)
    return vecOfTerms
    '''
    for key in d:
        addStringTermsToSet(setOfTerms, key)
        if type(d[key]) is dict:
            for t in getAllTerms(d[key]):
                addStringTermsToSet(setOfTerms, t)
        elif type(d[key]) is list:
            for t in d[key]:
                addStringTermsToSet(setOfTerms, t)
        else:
            try:
                addStringTermsToSet(setOfTerms, str(d[key].encode('utf-8')))
            except Exception as e:
                print str(e)
                continue

    return list(setOfTerms)
    ''' 

norm = Normalizador()
regex = re.compile('[^a-zA-Z0-9 ]')

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print 'Use python %s <XML> <TAG> <output>' % sys.argv[0]
        sys.exit(1)


    xml = sys.argv[1]
    tag = sys.argv[2]
    output = sys.argv[3]

    converter = XMLConverter(xml, tag)
    fout = open(output, 'w')
    cont = 0

    for d in converter.convertXMLToDict():
        #print d
        #print ' '.join(sorted(getAllTerms(d)))
        print >>fout, ' '.join(getAllTerms(d))
        cont += 1
        if cont % 10000 == 0: print cont, 'documentos processados...'
    
    print 'Total de %d documentos processados...' % cont

