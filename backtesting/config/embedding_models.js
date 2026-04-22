/**
 * Embedding model configurations for backtesting
 * Tests if different embeddings improve ensemble accuracy
 */

module.exports = {
  // Current baseline
  cohere_v3: {
    id: "cohere/embed-english-v3.0",
    provider: "cohere",
    model: "embed-english-v3.0",
    apiUrl: "https://api.cohere.com/v1/embed",
    dimensions: 1024,
    costPer1MTok: 0.00002,
    inputType: "search_document", // or "search_query"
    truncate: "END",
    description: "Current baseline - Cohere v3.0"
  },

  cohere_v2: {
    id: "cohere/embed-english-v2.0",
    provider: "cohere",
    model: "embed-english-v2.0",
    apiUrl: "https://api.cohere.com/v1/embed",
    dimensions: 768,
    costPer1MTok: 0.00001,
    description: "Older Cohere model - cheaper, lower dim"
  },

  openai_large: {
    id: "openai/text-embedding-3-large",
    provider: "openai",
    model: "text-embedding-3-large",
    apiUrl: "https://api.openai.com/v1/embeddings",
    dimensions: 3072,
    costPer1MTok: 0.000013,
    description: "OpenAI's largest embedding model"
  },

  openai_small: {
    id: "openai/text-embedding-3-small",
    provider: "openai",
    model: "text-embedding-3-small",
    apiUrl: "https://api.openai.com/v1/embeddings",
    dimensions: 1536,
    costPer1MTok: 0.0000020,
    description: "OpenAI's small embedding - fast & cheap"
  },

  jina: {
    id: "jina/jina-embeddings-v2-base-en",
    provider: "jina",
    model: "jina-embeddings-v2-base-en",
    apiUrl: "https://api.jina.ai/v1/embeddings",
    dimensions: 768,
    costPer1MTok: 0.0,
    description: "Jina's open-source embedding"
  },

  voyage: {
    id: "voyage/voyage-2",
    provider: "voyageai",
    model: "voyage-2",
    apiUrl: "https://api.voyageai.com/v1/embeddings",
    dimensions: 1024,
    costPer1MTok: 0.00001,
    description: "Voyage AI's embedding"
  }
};
