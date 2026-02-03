#!/usr/bin/env node

// 为老版本 Node.js 添加 AbortController polyfill
import AbortController from "abort-controller";
global.AbortController = global.AbortController || AbortController;

/**
 * Metabase MCP 服务器
 * 实现与 Metabase API 的交互，提供以下功能：
 * - 获取仪表板列表
 * - 获取问题列表
 * - 获取数据库列表
 * - 执行问题查询
 * - 获取仪表板详情
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListResourcesRequestSchema,
  ReadResourceRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import axios, { AxiosInstance } from "axios";
import { z } from "zod";

// 自定义错误枚举
enum ErrorCode {
  InternalError = "internal_error",
  InvalidRequest = "invalid_request",
  InvalidParams = "invalid_params",
  MethodNotFound = "method_not_found",
}

// 自定义错误类
class McpError extends Error {
  code: ErrorCode;

  constructor(code: ErrorCode, message: string) {
    super(message);
    this.code = code;
    this.name = "McpError";
  }
}

// 从环境变量获取 Metabase 配置
const METABASE_URL = process.env.METABASE_URL;
const METABASE_USERNAME = process.env.METABASE_USERNAME;
const METABASE_PASSWORD = process.env.METABASE_PASSWORD;
const METABASE_API_KEY = process.env.METABASE_API_KEY;

if (
  !METABASE_URL ||
  (!METABASE_API_KEY && (!METABASE_USERNAME || !METABASE_PASSWORD))
) {
  throw new Error(
    "Either (METABASE_URL and METABASE_API_KEY) or (METABASE_URL, METABASE_USERNAME, and METABASE_PASSWORD) environment variables are required",
  );
}

// 创建自定义 Schema 对象，使用 z.object
const ListResourceTemplatesRequestSchema = z.object({
  method: z.literal("resources/list_templates"),
});

const ListToolsRequestSchema = z.object({
  method: z.literal("tools/list"),
});

class MetabaseServer {
  private server: Server;
  private axiosInstance: AxiosInstance;
  private sessionToken: string | null = null;

  constructor() {
    this.server = new Server(
      {
        name: "metabase-server",
        version: "0.1.0",
      },
      {
        capabilities: {
          resources: {},
          tools: {},
        },
      },
    );

    this.axiosInstance = axios.create({
      baseURL: METABASE_URL,
      headers: {
        "Content-Type": "application/json",
      },
    });

    if (METABASE_API_KEY) {
      this.logInfo("Using Metabase API Key for authentication.");
      this.axiosInstance.defaults.headers.common["X-API-Key"] =
        METABASE_API_KEY;
      this.sessionToken = "api_key_used"; // Indicate API key is in use
    } else if (METABASE_USERNAME && METABASE_PASSWORD) {
      this.logInfo("Using Metabase username/password for authentication.");
      // Existing session token logic will apply
    } else {
      // This case should ideally be caught by the initial environment variable check
      // but as a safeguard:
      this.logError(
        "Metabase authentication credentials not configured properly.",
        {},
      );
      throw new Error(
        "Metabase authentication credentials not provided or incomplete.",
      );
    }

    this.setupResourceHandlers();
    this.setupToolHandlers();

    // Enhanced error handling with logging
    this.server.onerror = (error: Error) => {
      this.logError("Server Error", error);
    };

    process.on("SIGINT", async () => {
      this.logInfo("Shutting down server...");
      await this.server.close();
      process.exit(0);
    });
  }

  // Add logging utilities
  private logInfo(message: string, data?: unknown) {
    const logMessage = {
      timestamp: new Date().toISOString(),
      level: "info",
      message,
      data,
    };
    console.error(JSON.stringify(logMessage));
    // MCP SDK changed, can't directly access session
    try {
      // Use current session if available
      console.error(`INFO: ${message}`);
    } catch (e) {
      // Ignore if session not available
    }
  }

  private logError(message: string, error: unknown) {
    const errorObj = error as Error;
    const apiError = error as {
      response?: { data?: { message?: string } };
      message?: string;
    };

    const logMessage = {
      timestamp: new Date().toISOString(),
      level: "error",
      message,
      error: errorObj.message || "Unknown error",
      stack: errorObj.stack,
    };
    console.error(JSON.stringify(logMessage));
    // MCP SDK changed, can't directly access session
    try {
      console.error(
        `ERROR: ${message} - ${errorObj.message || "Unknown error"}`,
      );
    } catch (e) {
      // Ignore if session not available
    }
  }

  /**
   * 获取 Metabase 会话令牌
   */
  private async getSessionToken(): Promise<string> {
    if (this.sessionToken) {
      // Handles both API key ("api_key_used") and actual session tokens
      return this.sessionToken;
    }

    // This part should only be reached if using username/password and sessionToken is null
    this.logInfo("Authenticating with Metabase using username/password...");
    try {
      const response = await this.axiosInstance.post("/api/session", {
        username: METABASE_USERNAME,
        password: METABASE_PASSWORD,
      });

      this.sessionToken = response.data.id;

      // 设置默认请求头
      this.axiosInstance.defaults.headers.common["X-Metabase-Session"] =
        this.sessionToken;

      this.logInfo("Successfully authenticated with Metabase");
      return this.sessionToken as string;
    } catch (error) {
      this.logError("Authentication failed", error);
      throw new McpError(
        ErrorCode.InternalError,
        "Failed to authenticate with Metabase",
      );
    }
  }

  /**
   * 设置资源处理程序
   */
  private setupResourceHandlers() {
    this.server.setRequestHandler(
      ListResourcesRequestSchema,
      async (request) => {
        this.logInfo("Listing resources...", {
          requestStructure: JSON.stringify(request),
        });
        if (!METABASE_API_KEY) {
          await this.getSessionToken();
        }

        try {
          // 获取仪表板列表
          const dashboardsResponse =
            await this.axiosInstance.get("/api/dashboard");

          this.logInfo("Successfully listed resources", {
            count: dashboardsResponse.data.length,
          });
          // 将仪表板作为资源返回
          return {
            resources: dashboardsResponse.data.map((dashboard: any) => ({
              uri: `metabase://dashboard/${dashboard.id}`,
              mimeType: "application/json",
              name: dashboard.name,
              description: `Metabase dashboard: ${dashboard.name}`,
            })),
          };
        } catch (error) {
          this.logError("Failed to list resources", error);
          throw new McpError(
            ErrorCode.InternalError,
            "Failed to list Metabase resources",
          );
        }
      },
    );

    // 资源模板
    this.server.setRequestHandler(
      ListResourceTemplatesRequestSchema,
      async () => {
        return {
          resourceTemplates: [
            {
              uriTemplate: "metabase://dashboard/{id}",
              name: "Dashboard by ID",
              mimeType: "application/json",
              description: "Get a Metabase dashboard by its ID",
            },
            {
              uriTemplate: "metabase://card/{id}",
              name: "Card by ID",
              mimeType: "application/json",
              description: "Get a Metabase question/card by its ID",
            },
            {
              uriTemplate: "metabase://database/{id}",
              name: "Database by ID",
              mimeType: "application/json",
              description: "Get a Metabase database by its ID",
            },
          ],
        };
      },
    );

    // 读取资源
    this.server.setRequestHandler(
      ReadResourceRequestSchema,
      async (request) => {
        this.logInfo("Reading resource...", {
          requestStructure: JSON.stringify(request),
        });
        if (!METABASE_API_KEY) {
          await this.getSessionToken();
        }

        const uri = request.params?.uri;
        let match;

        try {
          // 处理仪表板资源
          if ((match = uri.match(/^metabase:\/\/dashboard\/(\d+)$/))) {
            const dashboardId = match[1];
            const response = await this.axiosInstance.get(
              `/api/dashboard/${dashboardId}`,
            );

            return {
              contents: [
                {
                  uri: request.params?.uri,
                  mimeType: "application/json",
                  text: JSON.stringify(response.data, null, 2),
                },
              ],
            };
          }

          // 处理问题/卡片资源
          else if ((match = uri.match(/^metabase:\/\/card\/(\d+)$/))) {
            const cardId = match[1];
            const response = await this.axiosInstance.get(
              `/api/card/${cardId}`,
            );

            return {
              contents: [
                {
                  uri: request.params?.uri,
                  mimeType: "application/json",
                  text: JSON.stringify(response.data, null, 2),
                },
              ],
            };
          }

          // 处理数据库资源
          else if ((match = uri.match(/^metabase:\/\/database\/(\d+)$/))) {
            const databaseId = match[1];
            const response = await this.axiosInstance.get(
              `/api/database/${databaseId}`,
            );

            return {
              contents: [
                {
                  uri: request.params?.uri,
                  mimeType: "application/json",
                  text: JSON.stringify(response.data, null, 2),
                },
              ],
            };
          } else {
            throw new McpError(
              ErrorCode.InvalidRequest,
              `Invalid URI format: ${uri}`,
            );
          }
        } catch (error) {
          if (axios.isAxiosError(error)) {
            throw new McpError(
              ErrorCode.InternalError,
              `Metabase API error: ${error.response?.data?.message || error.message}`,
            );
          }
          throw error;
        }
      },
    );
  }

  /**
   * 设置工具处理程序
   */
  private setupToolHandlers() {
    // No session token needed for listing tools, as it's static data
    this.server.setRequestHandler(ListToolsRequestSchema, async () => {
      return {
        tools: [
          {
            name: "list_dashboards",
            description: "List all dashboards in Metabase",
            inputSchema: {
              type: "object",
              properties: {},
            },
          },
          {
            name: "list_cards",
            description: "List all questions/cards in Metabase",
            inputSchema: {
              type: "object",
              properties: {
                f: {
                  type: "string",
                  description:
                    "Optional filter function, possible values: archived, table, database, using_model, bookmarked, using_segment, all, mine",
                },
              },
            },
          },
          {
            name: "list_databases",
            description: "List all databases in Metabase",
            inputSchema: {
              type: "object",
              properties: {},
            },
          },
          {
            name: "list_collections",
            description: "List all collections in Metabase",
            inputSchema: {
              type: "object",
              properties: {},
            },
          },
          {
            name: "get_database",
            description:
              "Get detailed information about a specific Metabase database including tables and schema",
            inputSchema: {
              type: "object",
              properties: {
                database_id: {
                  type: "number",
                  description: "ID of the database",
                },
              },
              required: ["database_id"],
            },
          },
          {
            name: "get_database_metadata",
            description:
              "Get complete metadata for a database including all tables, fields, and schema information",
            inputSchema: {
              type: "object",
              properties: {
                database_id: {
                  type: "number",
                  description: "ID of the database",
                },
              },
              required: ["database_id"],
            },
          },
          {
            name: "execute_card",
            description: "Execute a Metabase question/card and get results",
            inputSchema: {
              type: "object",
              properties: {
                card_id: {
                  type: "number",
                  description: "ID of the card/question to execute",
                },
                parameters: {
                  type: "object",
                  description: "Optional parameters for the query",
                },
              },
              required: ["card_id"],
            },
          },
          {
            name: "get_dashboard_cards",
            description: "Get all cards in a dashboard",
            inputSchema: {
              type: "object",
              properties: {
                dashboard_id: {
                  type: "number",
                  description: "ID of the dashboard",
                },
              },
              required: ["dashboard_id"],
            },
          },
          {
            name: "execute_query",
            description:
              "Execute a SQL query against a Metabase database, or MongoDB aggregation pipeline against MongoDB databases",
            inputSchema: {
              type: "object",
              properties: {
                database_id: {
                  type: "number",
                  description: "ID of the database to query",
                },
                query: {
                  type: "string",
                  description:
                    "SQL query for SQL databases, or MongoDB aggregation pipeline as JSON string (e.g., '[{\"$limit\": 10}]') for MongoDB databases",
                },
                collection: {
                  type: "string",
                  description:
                    "MongoDB collection name (required for MongoDB databases, e.g., 'kpj-user-profiles'). Ignored for SQL databases.",
                },
                native_parameters: {
                  type: "array",
                  description: "Optional parameters for the query",
                  items: {
                    type: "object",
                  },
                },
              },
              required: ["database_id", "query"],
            },
          },
          {
            name: "create_card",
            description:
              'Create a new Metabase question (card) that will appear in collections. For dashboard-only cards, use create_dashboard_only_card instead. For MongoDB queries, use format: {"database": 4, "lib/type": "mbql/query", "stages": [{"collection": "collection-name", "lib/type": "mbql.stage/native", "native": "[{...}]"}]}. For MongoDB date filtering with template tags: use string comparison instead of date objects. Convert dates to ISO strings with $dateToString, then use template tags WITHOUT quotes in the query (e.g., {"$gte": {{date_start}}}). Set template tag defaults WITH quotes (e.g., default: \'"2020-01-01T00:00:00.000Z"\'). This allows Metabase to properly substitute date values from dashboard parameters.',
            inputSchema: {
              type: "object",
              properties: {
                name: { type: "string", description: "Name of the card" },
                dataset_query: {
                  type: "object",
                  additionalProperties: true,
                  description:
                    'The query for the card. For MongoDB: {"database": 4, "lib/type": "mbql/query", "stages": [{"collection": "collection-name", "lib/type": "mbql.stage/native", "native": "[{...}]"}]}',
                },
                display: {
                  type: "string",
                  description:
                    "Display type (e.g., 'table', 'line', 'bar', 'pie', 'scalar')",
                },
                visualization_settings: {
                  type: "object",
                  additionalProperties: true,
                  description:
                    'Settings for the visualization (e.g., {"graph.dimensions": ["field"], "graph.metrics": ["count"]})',
                },
                collection_id: {
                  type: "number",
                  description:
                    "Optional ID of the collection to save the card in",
                },
                description: {
                  type: "string",
                  description: "Optional description for the card",
                },
              },
              required: [
                "name",
                "dataset_query",
                "display",
                "visualization_settings",
              ],
            },
          },
          {
            name: "update_card",
            description:
              'Update an existing Metabase question (card). For MongoDB date filtering with template tags: use string comparison instead of date objects. Convert dates to ISO strings with $dateToString, then use template tags WITHOUT quotes in the query (e.g., {"$gte": {{date_start}}}). Set template tag defaults WITH quotes (e.g., default: \'"2020-01-01T00:00:00.000Z"\'). This allows Metabase to properly substitute date values from dashboard parameters.',
            inputSchema: {
              type: "object",
              properties: {
                card_id: {
                  type: "number",
                  description: "ID of the card to update",
                },
                name: { type: "string", description: "New name for the card" },
                dataset_query: {
                  type: "object",
                  description: "New query for the card",
                },
                display: { type: "string", description: "New display type" },
                visualization_settings: {
                  type: "object",
                  description: "New visualization settings",
                },
                collection_id: {
                  type: "number",
                  description: "New collection ID",
                },
                description: { type: "string", description: "New description" },
                archived: {
                  type: "boolean",
                  description: "Set to true to archive the card",
                },
              },
              required: ["card_id"],
            },
          },
          {
            name: "delete_card",
            description: "Delete a Metabase question (card).",
            inputSchema: {
              type: "object",
              properties: {
                card_id: {
                  type: "number",
                  description: "ID of the card to delete",
                },
                hard_delete: {
                  type: "boolean",
                  description:
                    "Set to true for hard delete, false (default) for archive",
                  default: false,
                },
              },
              required: ["card_id"],
            },
          },
          {
            name: "create_dashboard",
            description: "Create a new Metabase dashboard.",
            inputSchema: {
              type: "object",
              properties: {
                name: { type: "string", description: "Name of the dashboard" },
                description: {
                  type: "string",
                  description: "Optional description for the dashboard",
                },
                parameters: {
                  type: "array",
                  description: "Optional parameters for the dashboard",
                  items: { type: "object" },
                },
                collection_id: {
                  type: "number",
                  description:
                    "Optional ID of the collection to save the dashboard in",
                },
              },
              required: ["name"],
            },
          },
          {
            name: "update_dashboard",
            description: "Update an existing Metabase dashboard.",
            inputSchema: {
              type: "object",
              properties: {
                dashboard_id: {
                  type: "number",
                  description: "ID of the dashboard to update",
                },
                name: {
                  type: "string",
                  description: "New name for the dashboard",
                },
                description: {
                  type: "string",
                  description: "New description for the dashboard",
                },
                parameters: {
                  type: "array",
                  description: "New parameters for the dashboard",
                  items: { type: "object" },
                },
                collection_id: {
                  type: "number",
                  description: "New collection ID",
                },
                archived: {
                  type: "boolean",
                  description: "Set to true to archive the dashboard",
                },
              },
              required: ["dashboard_id"],
            },
          },
          {
            name: "delete_dashboard",
            description: "Delete a Metabase dashboard.",
            inputSchema: {
              type: "object",
              properties: {
                dashboard_id: {
                  type: "number",
                  description: "ID of the dashboard to delete",
                },
                hard_delete: {
                  type: "boolean",
                  description:
                    "Set to true for hard delete, false (default) for archive",
                  default: false,
                },
              },
              required: ["dashboard_id"],
            },
          },
          {
            name: "add_card_to_dashboard",
            description: "Add an existing card to a dashboard.",
            inputSchema: {
              type: "object",
              properties: {
                dashboard_id: {
                  type: "number",
                  description: "ID of the dashboard to add the card to",
                },
                card_id: {
                  type: "number",
                  description: "ID of the card to add",
                },
                row: {
                  type: "number",
                  description: "Row position (default: 0)",
                  default: 0,
                },
                col: {
                  type: "number",
                  description: "Column position (default: 0)",
                  default: 0,
                },
                size_x: {
                  type: "number",
                  description: "Width in grid units (default: 4)",
                  default: 4,
                },
                size_y: {
                  type: "number",
                  description: "Height in grid units (default: 4)",
                  default: 4,
                },
                dashboard_tab_id: {
                  type: "number",
                  description:
                    "ID of the dashboard tab to add the card to (optional)",
                },
              },
              required: ["dashboard_id", "card_id"],
            },
          },
          {
            name: "remove_card_from_dashboard",
            description:
              "Remove a card from a dashboard (does not delete the card itself, just removes it from the dashboard).",
            inputSchema: {
              type: "object",
              properties: {
                dashboard_id: {
                  type: "number",
                  description: "ID of the dashboard",
                },
                dashcard_id: {
                  type: "number",
                  description:
                    "ID of the dashboard card (dashcard) to remove. Use get_dashboard_cards to find this ID.",
                },
              },
              required: ["dashboard_id", "dashcard_id"],
            },
          },
          {
            name: "update_dashboard_card",
            description:
              'Update the position, size, or parameter mappings of a card in a dashboard. For date filtering: ensure card template tags are configured correctly (see create_card/update_card descriptions), then use parameter_mappings to connect dashboard date parameters to card template tags. Example mapping: [{"parameter_id": "date_start_param", "target": ["variable", ["template-tag", "date_start"]], "card_id": 99}]',
            inputSchema: {
              type: "object",
              properties: {
                dashboard_id: {
                  type: "number",
                  description: "ID of the dashboard",
                },
                dashcard_id: {
                  type: "number",
                  description: "ID of the dashboard card to update",
                },
                row: {
                  type: "number",
                  description: "New row position",
                },
                col: {
                  type: "number",
                  description: "New column position",
                },
                size_x: {
                  type: "number",
                  description: "New width in grid units",
                },
                size_y: {
                  type: "number",
                  description: "New height in grid units",
                },
                parameter_mappings: {
                  type: "array",
                  description:
                    "Parameter mappings to connect dashboard filters to card template tags",
                  items: {
                    type: "object",
                  },
                },
              },
              required: ["dashboard_id", "dashcard_id"],
            },
          },
          {
            name: "create_dashboard_only_card",
            description:
              'Create a virtual card that exists only within a dashboard and does not appear in any collection. This is useful for dashboard-specific visualizations. For MongoDB queries, use format: {"database": 4, "lib/type": "mbql/query", "stages": [{"collection": "collection-name", "lib/type": "mbql.stage/native", "native": "[{...}]"}]}. For MongoDB date filtering with template tags: use string comparison instead of date objects. Convert dates to ISO strings with $dateToString, then use template tags WITHOUT quotes in the query (e.g., {"$gte": {{date_start}}}). Set template tag defaults WITH quotes (e.g., default: \'"2020-01-01T00:00:00.000Z"\'). This allows Metabase to properly substitute date values from dashboard parameters.',
            inputSchema: {
              type: "object",
              properties: {
                dashboard_id: {
                  type: "number",
                  description: "ID of the dashboard to add the card to",
                },
                name: {
                  type: "string",
                  description: "Name of the card",
                },
                dataset_query: {
                  type: "object",
                  additionalProperties: true,
                  description:
                    'The query for the card. For MongoDB: {"database": 4, "lib/type": "mbql/query", "stages": [{"collection": "collection-name", "lib/type": "mbql.stage/native", "native": "[{...}]"}]}',
                },
                display: {
                  type: "string",
                  description:
                    "Display type (e.g., 'table', 'line', 'bar', 'pie', 'scalar')",
                },
                visualization_settings: {
                  type: "object",
                  additionalProperties: true,
                  description:
                    'Settings for the visualization (e.g., {"graph.dimensions": ["field"], "graph.metrics": ["count"]})',
                },
                row: {
                  type: "number",
                  description: "Row position (default: 0)",
                  default: 0,
                },
                col: {
                  type: "number",
                  description: "Column position (default: 0)",
                  default: 0,
                },
                size_x: {
                  type: "number",
                  description: "Width in grid units (default: 4)",
                  default: 4,
                },
                size_y: {
                  type: "number",
                  description: "Height in grid units (default: 4)",
                  default: 4,
                },
              },
              required: [
                "dashboard_id",
                "name",
                "dataset_query",
                "display",
                "visualization_settings",
              ],
            },
          },
        ],
      };
    });

    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      this.logInfo("Calling tool...", {
        toolName: request.params?.name,
        arguments: request.params?.arguments,
        fullRequest: JSON.stringify(request),
      });
      if (!METABASE_API_KEY) {
        await this.getSessionToken();
      }

      try {
        switch (request.params?.name) {
          case "list_dashboards": {
            const response = await this.axiosInstance.get("/api/dashboard");
            return {
              content: [
                {
                  type: "text",
                  text: JSON.stringify(response.data, null, 2),
                },
              ],
            };
          }

          case "list_cards": {
            const f = request.params?.arguments?.f || "all";
            const response = await this.axiosInstance.get(`/api/card?f=${f}`);
            return {
              content: [
                {
                  type: "text",
                  text: JSON.stringify(response.data, null, 2),
                },
              ],
            };
          }

          case "list_databases": {
            const response = await this.axiosInstance.get("/api/database");
            return {
              content: [
                {
                  type: "text",
                  text: JSON.stringify(response.data, null, 2),
                },
              ],
            };
          }

          case "list_collections": {
            const response = await this.axiosInstance.get("/api/collection");
            return {
              content: [
                {
                  type: "text",
                  text: JSON.stringify(response.data, null, 2),
                },
              ],
            };
          }

          case "get_database": {
            const databaseId = request.params?.arguments?.database_id;
            if (!databaseId) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Database ID is required",
              );
            }

            const response = await this.axiosInstance.get(
              `/api/database/${databaseId}`,
            );
            return {
              content: [
                {
                  type: "text",
                  text: JSON.stringify(response.data, null, 2),
                },
              ],
            };
          }

          case "get_database_metadata": {
            const databaseId = request.params?.arguments?.database_id;
            if (!databaseId) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Database ID is required",
              );
            }

            const response = await this.axiosInstance.get(
              `/api/database/${databaseId}/metadata`,
            );

            // Filter to only include table names/IDs and field names/IDs
            const filteredData = {
              id: response.data.id,
              name: response.data.name,
              tables:
                response.data.tables?.map((table: any) => ({
                  id: table.id,
                  name: table.name,
                  fields:
                    table.fields?.map((field: any) => ({
                      id: field.id,
                      name: field.name,
                      database_type: field.database_type,
                    })) || [],
                })) || [],
            };

            return {
              content: [
                {
                  type: "text",
                  text: JSON.stringify(filteredData, null, 2),
                },
              ],
            };
          }

          case "execute_card": {
            const cardId = request.params?.arguments?.card_id;
            if (!cardId) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Card ID is required",
              );
            }

            const parameters = request.params?.arguments?.parameters || {};
            const response = await this.axiosInstance.post(
              `/api/card/${cardId}/query`,
              { parameters },
            );

            return {
              content: [
                {
                  type: "text",
                  text: JSON.stringify(response.data, null, 2),
                },
              ],
            };
          }

          case "get_dashboard_cards": {
            const dashboardId = request.params?.arguments?.dashboard_id;
            if (!dashboardId) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Dashboard ID is required",
              );
            }

            const response = await this.axiosInstance.get(
              `/api/dashboard/${dashboardId}`,
            );

            // Return dashcards (which is the correct field name in Metabase API)
            const dashcards =
              response.data.dashcards || response.data.cards || [];

            return {
              content: [
                {
                  type: "text",
                  text: JSON.stringify(dashcards, null, 2),
                },
              ],
            };
          }

          case "execute_query": {
            const databaseId = request.params?.arguments?.database_id;
            const query = request.params?.arguments?.query;
            const collectionParam = request.params?.arguments?.collection;
            const nativeParameters =
              request.params?.arguments?.native_parameters || [];

            if (!databaseId) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Database ID is required",
              );
            }

            if (!query) {
              throw new McpError(ErrorCode.InvalidParams, "Query is required");
            }

            // Get database details to check engine type
            const dbResponse = await this.axiosInstance.get(
              `/api/database/${databaseId}`,
            );
            const dbEngine = dbResponse.data.engine;

            let queryData;

            if (dbEngine === "mongo") {
              // MongoDB query format
              if (!collectionParam) {
                throw new McpError(
                  ErrorCode.InvalidParams,
                  "Collection name is required for MongoDB queries",
                );
              }

              const queryStr = String(query);

              queryData = {
                type: "native",
                native: {
                  collection: collectionParam,
                  query: queryStr,
                  template_tags: {},
                },
                parameters: nativeParameters,
                database: databaseId,
              };
            } else {
              // SQL query format
              queryData = {
                type: "native",
                native: {
                  query: query,
                  template_tags: {},
                },
                parameters: nativeParameters,
                database: databaseId,
              };
            }

            const response = await this.axiosInstance.post(
              "/api/dataset",
              queryData,
            );

            return {
              content: [
                {
                  type: "text",
                  text: JSON.stringify(response.data, null, 2),
                },
              ],
            };
          }

          case "create_card": {
            const {
              name,
              dataset_query,
              display,
              visualization_settings,
              collection_id,
              description,
            } = request.params?.arguments || {};
            if (
              !name ||
              !dataset_query ||
              !display ||
              !visualization_settings
            ) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Missing required fields for create_card: name, dataset_query, display, visualization_settings",
              );
            }
            const createCardBody: any = {
              name,
              dataset_query,
              display,
              visualization_settings,
            };
            if (collection_id !== undefined)
              createCardBody.collection_id = collection_id;
            if (description !== undefined)
              createCardBody.description = description;

            const response = await this.axiosInstance.post(
              "/api/card",
              createCardBody,
            );

            // Add a user-friendly link to view the card
            const cardId = response.data.id;
            const cardLink = `${METABASE_URL}/question/${cardId}`;
            const resultWithLink = {
              ...response.data,
              _link: cardLink,
              _message: `Card created successfully! View it at: ${cardLink}`,
            };

            return {
              content: [
                {
                  type: "text",
                  text: JSON.stringify(resultWithLink, null, 2),
                },
              ],
            };
          }

          case "update_card": {
            const { card_id, ...updateFields } =
              request.params?.arguments || {};
            if (!card_id) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Card ID is required for update_card",
              );
            }
            if (Object.keys(updateFields).length === 0) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "No fields provided for update_card",
              );
            }
            const response = await this.axiosInstance.put(
              `/api/card/${card_id}`,
              updateFields,
            );
            return {
              content: [
                {
                  type: "text",
                  text: JSON.stringify(response.data, null, 2),
                },
              ],
            };
          }

          case "delete_card": {
            const { card_id, hard_delete = false } =
              request.params?.arguments || {};
            if (!card_id) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Card ID is required for delete_card",
              );
            }

            if (hard_delete) {
              await this.axiosInstance.delete(`/api/card/${card_id}`);
              return {
                content: [
                  {
                    type: "text",
                    text: `Card ${card_id} permanently deleted.`,
                  },
                ],
              };
            } else {
              // Soft delete (archive)
              const response = await this.axiosInstance.put(
                `/api/card/${card_id}`,
                { archived: true },
              );
              return {
                content: [
                  {
                    type: "text",
                    // Metabase might return the updated card object or just a success status.
                    // If response.data is available and meaningful, include it. Otherwise, a generic success message.
                    text: response.data
                      ? `Card ${card_id} archived. Details: ${JSON.stringify(response.data, null, 2)}`
                      : `Card ${card_id} archived.`,
                  },
                ],
              };
            }
          }

          case "create_dashboard": {
            const { name, description, parameters, collection_id } =
              request.params?.arguments || {};
            if (!name) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Missing required field for create_dashboard: name",
              );
            }
            const createDashboardBody: any = { name };
            if (description !== undefined)
              createDashboardBody.description = description;
            if (parameters !== undefined)
              createDashboardBody.parameters = parameters;
            if (collection_id !== undefined)
              createDashboardBody.collection_id = collection_id;

            const response = await this.axiosInstance.post(
              "/api/dashboard",
              createDashboardBody,
            );

            // Add a user-friendly link to view the dashboard
            const dashboardId = response.data.id;
            const dashboardLink = `${METABASE_URL}/dashboard/${dashboardId}`;
            const resultWithLink = {
              ...response.data,
              _link: dashboardLink,
              _message: `Dashboard created successfully! View it at: ${dashboardLink}`,
            };

            return {
              content: [
                {
                  type: "text",
                  text: JSON.stringify(resultWithLink, null, 2),
                },
              ],
            };
          }

          case "update_dashboard": {
            const { dashboard_id, ...updateFields } =
              request.params?.arguments || {};
            if (!dashboard_id) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Dashboard ID is required for update_dashboard",
              );
            }
            if (Object.keys(updateFields).length === 0) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "No fields provided for update_dashboard",
              );
            }
            const response = await this.axiosInstance.put(
              `/api/dashboard/${dashboard_id}`,
              updateFields,
            );
            return {
              content: [
                {
                  type: "text",
                  text: JSON.stringify(response.data, null, 2),
                },
              ],
            };
          }

          case "delete_dashboard": {
            const { dashboard_id, hard_delete = false } =
              request.params?.arguments || {};
            if (!dashboard_id) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Dashboard ID is required for delete_dashboard",
              );
            }

            if (hard_delete) {
              await this.axiosInstance.delete(`/api/dashboard/${dashboard_id}`);
              return {
                content: [
                  {
                    type: "text",
                    text: `Dashboard ${dashboard_id} permanently deleted.`,
                  },
                ],
              };
            } else {
              // Soft delete (archive)
              const response = await this.axiosInstance.put(
                `/api/dashboard/${dashboard_id}`,
                { archived: true },
              );
              return {
                content: [
                  {
                    type: "text",
                    text: response.data
                      ? `Dashboard ${dashboard_id} archived. Details: ${JSON.stringify(response.data, null, 2)}`
                      : `Dashboard ${dashboard_id} archived.`,
                  },
                ],
              };
            }
          }

          case "add_card_to_dashboard": {
            const {
              dashboard_id,
              card_id,
              row = 0,
              col = 0,
              size_x = 4,
              size_y = 4,
            } = request.params?.arguments || {};

            if (!dashboard_id) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Dashboard ID is required for add_card_to_dashboard",
              );
            }

            if (!card_id) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Card ID is required for add_card_to_dashboard",
              );
            }

            // First, get existing dashboard to retrieve current cards
            const dashboardResponse = await this.axiosInstance.get(
              `/api/dashboard/${dashboard_id}`,
            );

            // Extract existing cards (dashcards)
            const existingCards = dashboardResponse.data.dashcards || [];

            // Map existing cards to the format needed for PUT
            const existingCardsFormatted = existingCards.map((dc: any) => ({
              id: dc.id,
              card_id: dc.card_id,
              row: dc.row,
              col: dc.col,
              size_x: dc.size_x,
              size_y: dc.size_y,
              series: dc.series || [],
              visualization_settings: dc.visualization_settings || {},
              parameter_mappings: dc.parameter_mappings || [],
            }));

            // Add the new card with id=-1
            const allCards = [
              ...existingCardsFormatted,
              {
                id: -1,
                card_id: card_id,
                row: row,
                col: col,
                size_x: size_x,
                size_y: size_y,
              },
            ];

            // Metabase API requires PUT with all cards
            const updateBody = {
              cards: allCards,
            };

            const response = await this.axiosInstance.put(
              `/api/dashboard/${dashboard_id}/cards`,
              updateBody,
            );

            return {
              content: [
                {
                  type: "text",
                  text: JSON.stringify(response.data, null, 2),
                },
              ],
            };
          }

          case "remove_card_from_dashboard": {
            const { dashboard_id, dashcard_id } =
              request.params?.arguments || {};

            if (!dashboard_id) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Dashboard ID is required for remove_card_from_dashboard",
              );
            }

            if (!dashcard_id) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Dashcard ID is required for remove_card_from_dashboard",
              );
            }

            await this.axiosInstance.delete(
              `/api/dashboard/${dashboard_id}/cards/${dashcard_id}`,
            );

            return {
              content: [
                {
                  type: "text",
                  text: `Card ${dashcard_id} removed from dashboard ${dashboard_id}.`,
                },
              ],
            };
          }

          case "update_dashboard_card": {
            const {
              dashboard_id,
              dashcard_id,
              parameter_mappings,
              ...updateFields
            } = request.params?.arguments || {};

            if (!dashboard_id) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Dashboard ID is required for update_dashboard_card",
              );
            }

            if (!dashcard_id) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Dashcard ID is required for update_dashboard_card",
              );
            }

            if (
              Object.keys(updateFields).length === 0 &&
              parameter_mappings === undefined
            ) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "No fields provided for update_dashboard_card",
              );
            }

            // First, get existing dashboard to retrieve current cards
            const dashboardResponse = await this.axiosInstance.get(
              `/api/dashboard/${dashboard_id}`,
            );

            // Extract existing cards (dashcards)
            const existingCards = dashboardResponse.data.dashcards || [];

            // Find and update the specific card
            const updatedCards = existingCards.map((dc: any) => {
              if (dc.id === dashcard_id) {
                // Update this card with new fields
                return {
                  id: dc.id,
                  card_id: dc.card_id,
                  row:
                    updateFields.row !== undefined ? updateFields.row : dc.row,
                  col:
                    updateFields.col !== undefined ? updateFields.col : dc.col,
                  size_x:
                    updateFields.size_x !== undefined
                      ? updateFields.size_x
                      : dc.size_x,
                  size_y:
                    updateFields.size_y !== undefined
                      ? updateFields.size_y
                      : dc.size_y,
                  series: dc.series || [],
                  visualization_settings: dc.visualization_settings || {},
                  parameter_mappings:
                    parameter_mappings !== undefined
                      ? parameter_mappings
                      : dc.parameter_mappings || [],
                };
              }
              // Keep other cards as-is
              return {
                id: dc.id,
                card_id: dc.card_id,
                row: dc.row,
                col: dc.col,
                size_x: dc.size_x,
                size_y: dc.size_y,
                series: dc.series || [],
                visualization_settings: dc.visualization_settings || {},
                parameter_mappings: dc.parameter_mappings || [],
              };
            });

            // PUT all cards back
            const updateBody = {
              cards: updatedCards,
            };

            const response = await this.axiosInstance.put(
              `/api/dashboard/${dashboard_id}/cards`,
              updateBody,
            );

            return {
              content: [
                {
                  type: "text",
                  text: JSON.stringify(response.data, null, 2),
                },
              ],
            };
          }

          case "create_dashboard_only_card": {
            // Debug logging
            this.logInfo("create_dashboard_only_card called", {
              arguments: request.params?.arguments,
            });

            const {
              dashboard_id,
              name,
              dataset_query,
              display,
              visualization_settings,
              row = 0,
              col = 0,
              size_x = 4,
              size_y = 4,
            } = request.params?.arguments || {};

            this.logInfo("Extracted parameters", {
              dashboard_id,
              name,
              dataset_query,
              display,
              visualization_settings,
              row,
              col,
              size_x,
              size_y,
            });

            if (!dashboard_id) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Dashboard ID is required for create_dashboard_only_card",
              );
            }

            if (
              !name ||
              !dataset_query ||
              !display ||
              !visualization_settings
            ) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Missing required fields: name, dataset_query, display, visualization_settings",
              );
            }

            // Get existing dashboard to retrieve current cards
            const dashboardResponse = await this.axiosInstance.get(
              `/api/dashboard/${dashboard_id}`,
            );

            const existingCards = dashboardResponse.data.dashcards || [];

            // Format existing cards
            const existingCardsFormatted = existingCards.map((dc: any) => ({
              id: dc.id,
              card_id: dc.card_id,
              row: dc.row,
              col: dc.col,
              size_x: dc.size_x,
              size_y: dc.size_y,
              series: dc.series || [],
              visualization_settings: dc.visualization_settings || {},
              parameter_mappings: dc.parameter_mappings || [],
            }));

            // Create virtual card (dashboard-only card)
            const virtualCard = {
              id: -1,
              card_id: null, // null means it's a virtual card
              row: row,
              col: col,
              size_x: size_x,
              size_y: size_y,
              series: [],
              parameter_mappings: [],
              visualization_settings: {
                ...visualization_settings,
                virtual_card: {
                  name: name,
                  display: display,
                  visualization_settings: visualization_settings,
                  dataset_query: dataset_query,
                },
              },
            };

            const allCards = [...existingCardsFormatted, virtualCard];

            // Update dashboard with new virtual card
            const updateBody = {
              cards: allCards,
            };

            const response = await this.axiosInstance.put(
              `/api/dashboard/${dashboard_id}/cards`,
              updateBody,
            );

            return {
              content: [
                {
                  type: "text",
                  text: JSON.stringify(response.data, null, 2),
                },
              ],
            };
          }

          default:
            return {
              content: [
                {
                  type: "text",
                  text: `Unknown tool: ${request.params?.name}`,
                },
              ],
              isError: true,
            };
        }
      } catch (error) {
        if (axios.isAxiosError(error)) {
          return {
            content: [
              {
                type: "text",
                text: `Metabase API error: ${error.response?.data?.message || error.message}`,
              },
            ],
            isError: true,
          };
        }
        throw error;
      }
    });
  }

  async run() {
    try {
      this.logInfo("Starting Metabase MCP server...");
      const transport = new StdioServerTransport();
      await this.server.connect(transport);
      this.logInfo("Metabase MCP server running on stdio");
    } catch (error) {
      this.logError("Failed to start server", error);
      throw error;
    }
  }
}

// Add global error handlers
process.on("uncaughtException", (error: Error) => {
  console.error(
    JSON.stringify({
      timestamp: new Date().toISOString(),
      level: "fatal",
      message: "Uncaught Exception",
      error: error.message,
      stack: error.stack,
    }),
  );
  process.exit(1);
});

process.on(
  "unhandledRejection",
  (reason: unknown, promise: Promise<unknown>) => {
    const errorMessage =
      reason instanceof Error ? reason.message : String(reason);
    console.error(
      JSON.stringify({
        timestamp: new Date().toISOString(),
        level: "fatal",
        message: "Unhandled Rejection",
        error: errorMessage,
      }),
    );
  },
);

const server = new MetabaseServer();
server.run().catch(console.error);
