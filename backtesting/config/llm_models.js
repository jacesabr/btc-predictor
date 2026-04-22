/**
 * LLM Model configurations for backtesting
 * Admin provides: model_name, api_key, (optional) api_base, temperature
 */

module.exports = {
  // Baseline model
  deepseek: {
    id: "deepseek-chat",
    provider: "deepseek",
    apiUrl: "https://api.deepseek.com/chat/completions",
    model: "deepseek-chat",
    contextWindow: 4096,
    costPerMTok: 0.0014,
    costPerOutputTok: 0.0020,
    defaultTemp: 0.1,
    description: "Current baseline model"
  },

  claude: {
    id: "claude-opus",
    provider: "anthropic",
    apiUrl: "https://api.anthropic.com/v1/messages",
    model: "claude-opus-4-1",
    contextWindow: 200000,
    costPer1MTok: 15.0,  // $15 per 1M input tokens
    costPerOutput1MTok: 75.0,
    defaultTemp: 0.1,
    description: "Anthropic's most capable model"
  },

  gpt4: {
    id: "gpt-4-turbo",
    provider: "openai",
    apiUrl: "https://api.openai.com/v1/chat/completions",
    model: "gpt-4-turbo-preview",
    contextWindow: 128000,
    costPerMTok: 0.01,
    costPerOutputMTok: 0.03,
    defaultTemp: 0.1,
    description: "OpenAI's GPT-4 Turbo"
  },

  gpt35: {
    id: "gpt-3.5-turbo",
    provider: "openai",
    apiUrl: "https://api.openai.com/v1/chat/completions",
    model: "gpt-3.5-turbo",
    contextWindow: 4096,
    costPerMTok: 0.0005,
    costPerOutputMTok: 0.0015,
    defaultTemp: 0.1,
    description: "OpenAI's faster, cheaper model"
  },

  cohere: {
    id: "command-r",
    provider: "cohere",
    apiUrl: "https://api.cohere.com/v1/chat",
    model: "command-r",
    contextWindow: 128000,
    costPerMTok: 0.0005,
    costPerOutputMTok: 0.0015,
    defaultTemp: 0.1,
    description: "Cohere's command model"
  },

  llama: {
    id: "llama2-70b",
    provider: "together",
    apiUrl: "https://api.together.xyz/v1/chat/completions",
    model: "meta-llama/Llama-2-70b-chat-hf",
    contextWindow: 4096,
    costPerMTok: 0.0009,
    costPerOutputMTok: 0.0009,
    defaultTemp: 0.1,
    description: "Open-source Llama 2 70B"
  }
};
