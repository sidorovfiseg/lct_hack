from langchain_community.embeddings import GigaChatEmbeddings


class GigaChatEmbeddingFunction:
    def __init__(self, credentials: str, scope: str):
        self.embeddings = GigaChatEmbeddings(credentials=credentials, verify_ssl_certs=False, scope=scope)

    def __call__(self, input):
        return self.embeddings.embed_documents(texts=input)
