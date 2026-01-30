/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.cloud.ai.dataagent.controller;

import com.alibaba.cloud.ai.dataagent.dto.GraphRequest;
import com.alibaba.cloud.ai.dataagent.service.graph.GraphService;
import com.alibaba.cloud.ai.dataagent.vo.GraphNodeResponse;
import jakarta.servlet.http.HttpServletResponse;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import static com.alibaba.cloud.ai.dataagent.constant.Constant.STREAM_EVENT_COMPLETE;
import static com.alibaba.cloud.ai.dataagent.constant.Constant.STREAM_EVENT_ERROR;

/**
 * 图谱执行控制器 (Graph Controller)
 * <p>
 * 该控制器作为 Data Agent 的核心入口，负责处理基于图谱的流式搜索请求。
 * 它采用 Server-Sent Events (SSE) 技术，将后端图谱节点的执行过程（如下发计划、生成的 SQL、执行结果等）
 * 实时推送给前端，提供交互式的用户体验。
 *
 * @author zhangshenghang
 * @author vlsmb
 */
@Slf4j
@RestController
@AllArgsConstructor
@CrossOrigin(origins = "*")
@RequestMapping("/api")
public class GraphController {

	private final GraphService graphService;

	/**
	 * 执行流式搜索任务
	 * <p>
	 * 接收用户的自然语言查询，初始化图谱执行上下文，并返回一个响应式流 (Flux) 以推送实时事件。
	 * 方法内部会通过 {@link GraphService} 异步启动图谱执行流程，并通过 {@link Sinks} 将事件桥接到响应流中。
	 *
	 * @param agentId              智能助手 ID，用于标识当前使用的 Agent 配置。
	 * @param threadId             会话线程 ID。如果为空，后端会自动生成一个新 ID。用于串联多轮对话上下文。
	 * @param query                用户的自然语言查询内容。
	 * @param humanFeedback        是否启用人工反馈 (Human-in-the-loop)。如果为
	 *                             true，图谱在关键节点会暂停等待用户确认。
	 * @param humanFeedbackContent 人工反馈的具体内容（例如用户修正后的计划或 SQL）。
	 * @param rejectedPlan         用户是否拒绝了当前生成的计划。如果为 true，图谱可能会触发重新规划 (Replanning)。
	 * @param nl2sqlOnly           调试标记。如果为 true，图谱仅执行到 NL2SQL 生成阶段，不执行后续的 SQL
	 *                             查询和可视化。
	 * @param response             {@link HttpServletResponse} 对象，用于手动设置 SSE 相关的响应头。
	 * @return {@link Flux} 包含 {@link ServerSentEvent} 的响应式流，用于向客户端持续推送图谱节点执行状态。
	 */
	@GetMapping(value = "/stream/search", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	public Flux<ServerSentEvent<GraphNodeResponse>> streamSearch(@RequestParam("agentId") String agentId,
			@RequestParam(value = "threadId", required = false) String threadId,
			@RequestParam("query") String query,
			@RequestParam(value = "humanFeedback", required = false) boolean humanFeedback,
			@RequestParam(value = "humanFeedbackContent", required = false) String humanFeedbackContent,
			@RequestParam(value = "rejectedPlan", required = false) boolean rejectedPlan,
			@RequestParam(value = "nl2sqlOnly", required = false) boolean nl2sqlOnly,
			HttpServletResponse response) {
		// 设置 SSE (Server-Sent Events) 相关的 HTTP 头
		// 必须禁用缓存并保持连接活跃，以支持长连接流式传输
		response.setCharacterEncoding("UTF-8");
		response.setContentType("text/event-stream");
		response.setHeader("Cache-Control", "no-cache");
		response.setHeader("Connection", "keep-alive");
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Headers", "Cache-Control");

		// 创建一个单播 (Unicast) 的 Sink，用于将命令式的数据推送转换为响应式的 Flux 流
		// onBackpressureBuffer 用于在下游消费速度慢于上游生产速度时缓冲事件，防止数据丢失
		Sinks.Many<ServerSentEvent<GraphNodeResponse>> sink = Sinks.many().unicast().onBackpressureBuffer();

		GraphRequest request = GraphRequest.builder()
				.agentId(agentId)
				.threadId(threadId)
				.query(query)
				.humanFeedback(humanFeedback)
				.humanFeedbackContent(humanFeedbackContent)
				.rejectedPlan(rejectedPlan)
				.nl2sqlOnly(nl2sqlOnly)
				.build();

		// 异步启动图谱处理流程。注意：此方法通常是异步非阻塞的，它会将事件推送到上述创建的 sink 中
		graphService.graphStreamProcess(sink, request);

		return sink.asFlux().filter(sse -> {
			// 过滤逻辑：确保只有有效的数据包才被推送到前端

			// 1. 如果是控制事件（完成或错误），无论是否有数据载荷，都必须透传给前端，以便前端正确关闭连接或报错
			if (STREAM_EVENT_COMPLETE.equals(sse.event()) || STREAM_EVENT_ERROR.equals(sse.event())) {
				return true;
			}
			// 2. 对于普通数据事件，过滤掉 null 或空文本的消息，避免前端收到无意义的空帧
			return sse.data() != null && sse.data().getText() != null && !sse.data().getText().isEmpty();
		})
				// 订阅钩子：记录日志，便于追踪会话开始
				.doOnSubscribe(
						subscription -> log.info("Client subscribed to stream, threadId: {}", request.getThreadId()))
				// 取消钩子：当客户端断开连接（如关闭页面）时触发
				// 重要：必须调用 stopStreamProcessing 以清理服务器端资源（如线程、上下文），防止内存泄漏
				.doOnCancel(() -> {
					log.info("Client disconnected from stream, threadId: {}", request.getThreadId());
					if (request.getThreadId() != null) {
						graphService.stopStreamProcessing(request.getThreadId());
					}
				})
				// 错误钩子：流处理发生异常时触发，同样需要清理资源
				.doOnError(e -> {
					log.error("Error occurred during streaming, threadId: {}: ", request.getThreadId(), e);
					if (request.getThreadId() != null) {
						graphService.stopStreamProcessing(request.getThreadId());
					}
				})
				// 完成钩子：流正常结束时触发
				.doOnComplete(() -> log.info("Stream completed successfully, threadId: {}", request.getThreadId()));
	}

}
