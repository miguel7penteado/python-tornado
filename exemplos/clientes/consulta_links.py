#!/usr/bin/env python
# coding=UTF-8

import time
from datetime import timedelta

try:
    from HTMLParser import HTMLParser
    from urlparse import urljoin, urldefrag
except ImportError:
    from codigo_html.parser import HTMLParser
    from urllib.parse import urljoin, urldefrag

from tornado import httpclient, gen, ioloop, queues

# vou pequisar esse endereço rodando apache.
endereco = 'http://172.16.0.11/'
concorrencia = 10


@gen.coroutine
def obter_links_de_enderecos(endereco):
    """Baixar a pagina `endereco` e esmiussa-la em busca de links.

    Os enderecos retornados tem o fragmento depois de `#` removido, 
    e sao refeitos em absoluto, como por exemplo
    o fragmento 'gen.codigo_html#tornado.gen.coroutine' se torna
    'http://www.tornadoweb.org/en/stable/gen.codigo_html'.
    """
    try:
        resposta = yield httpclient.AsyncHTTPClient().fetch(endereco)
        print('buscadas %s' % endereco)

        codigo_html = resposta.body if isinstance(resposta.body, str) \
            else resposta.body.decode()
        conjunto_enderecos = [urljoin(endereco, remover_fragmento(novo_endereco))
                for novo_endereco in obter_links(codigo_html)]
    except Exception as e:
        print('Problema: %s %s' % (e, endereco))
        raise gen.Return([])

    raise gen.Return(conjunto_enderecos)


def remover_fragmento(url):
    pure_url, frag = urldefrag(url)
    return pure_url


def obter_links(codigo_html):
    class URLSeeker(HTMLParser):
        def __init__(self):
            HTMLParser.__init__(self)
            self.conjunto_enderecos = []

        def handle_starttag(self, tag, attrs):
            href = dict(attrs).get('href')
            if href and tag == 'a':
                self.conjunto_enderecos.append(href)

    url_seeker = URLSeeker()
    url_seeker.feed(codigo_html)
    return url_seeker.conjunto_enderecos


@gen.coroutine
def main():
    fila_execucao = queues.Queue()
    tempo_de_inicio = time.time()
    buscando, pesquisado = set(), set()

    @gen.coroutine
    def pesquisar_endereco():
        endereco_atual = yield fila_execucao.get()
        try:
            if endereco_atual in buscando:
                return

            print('buscando %s' % endereco_atual)
            buscando.add(endereco_atual)
            conjunto_enderecos = yield obter_links_de_enderecos(endereco_atual)
            pesquisado.add(endereco_atual)

            for novo_endereco in conjunto_enderecos:
                # Apenas pesquisa links abaixo da URL base
                if novo_endereco.startswith(endereco):
                    yield fila_execucao.put(novo_endereco)

        finally:
            fila_execucao.task_done()

    @gen.coroutine
    def executor():
        while True:
            yield pesquisar_endereco()

    fila_execucao.put(endereco)

    # Ativar executores, então esperar a fila de executores estar vazia.
    for _ in range(concorrencia):
        executor()
    yield fila_execucao.join(timeout=timedelta(seconds=300))
    assert buscando == pesquisado
    print('Feito em %d segundos, obtidas %s URLs.' % (time.time() - tempo_de_inicio, len(pesquisado)))


if __name__ == '__main__':
    import logging
    logging.basicConfig()
    laco_de_transacao_dados = ioloop.IOLoop.current()
    laco_de_transacao_dados.run_sync(main)
