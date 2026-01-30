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
package com.alibaba.cloud.ai.dataagent.service.graph.Context;

import com.alibaba.cloud.ai.dataagent.properties.DataAgentProperties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 多轮对话上下文管理器 (MultiTurnContextManager)
 * <p>
 * 负责管理每个会话线程的轻量级历史记录。它会保存用户的提问以及对应的 Planner 输出，
 * 以便后续的 Prompt 能够引用之前的对话内容（Context），从而实现连贯的多轮对话体验。
 * </p>
 * 
 * @see com.alibaba.cloud.ai.dataagent.service.graph.node.PlannerNode
 */
@Slf4j
@Component
@AllArgsConstructor
public class MultiTurnContextManager {

	private final DataAgentProperties properties;

	// todo：考虑持久化存储
	// 使用 ConcurrentHashMap 存储每个线程的历史记录，确保线程安全
	private final Map<String, Deque<ConversationTurn>> history = new ConcurrentHashMap<>();

	// 暂存当前正在进行的轮次数据，直到该轮次结束才移入 history
	private final Map<String, PendingTurn> pendingTurns = new ConcurrentHashMap<>();

	/**
	 * 开始记录新的对话轮次
	 * <p>
	 * 当用户发起新的提问时调用。系统会创建一个 `PendingTurn` 对象暂存该问题，
	 * 等待后续 Planner 生成计划并填充进来。
	 * </p>
	 * 
	 * @param threadId     会话线程 ID，用于标识当前对话流
	 * @param userQuestion 用户最新的提问内容
	 */
	public void beginTurn(String threadId, String userQuestion) {
		if (StringUtils.isAnyBlank(threadId, userQuestion)) {
			return;
		}
		pendingTurns.put(threadId, new PendingTurn(userQuestion.trim()));
	}

	/**
	 * 追加 Planner 的输出片段
	 * <p>
	 * 由于 Planner 的输出通常是流式的（Streaming），该方法用于接收并累积这些流式片段。
	 * 只有完整的计划生成后，才有价值作为历史上下文保存。
	 * </p>
	 * 
	 * @param threadId 会话线程 ID
	 * @param chunk    Planner 生成的流式文本片段
	 */
	public void appendPlannerChunk(String threadId, String chunk) {
		if (StringUtils.isAnyBlank(threadId, chunk)) {
			return;
		}
		PendingTurn pending = pendingTurns.get(threadId);
		if (pending != null) {
			pending.planBuilder.append(chunk);
		}
	}

	/**
	 * 结束当前轮次并归档到历史记录
	 * <p>
	 * 当一次完整的请求处理结束后（如 Stream 完成），将暂存的 `PendingTurn` 移入 `history`。
	 * 为了防止上下文过长导致 Prompt 超出 Token 限制，这里会应用两个策略：
	 * 1. 截断过长的计划文本 (MaxPlanLength)。
	 * 2. 移除最早的历史记录，保持固定窗口大小 (MaxTurnHistory)。
	 * </p>
	 * 
	 * @param threadId 会话线程 ID
	 */
	public void finishTurn(String threadId) {
		PendingTurn pending = pendingTurns.remove(threadId);
		if (pending == null) {
			return;
		}
		String plan = StringUtils.trimToEmpty(pending.planBuilder.toString());
		if (StringUtils.isBlank(plan)) {
			log.debug("No planner output recorded for thread {}, skipping history update", threadId);
			return;
		}

		String trimmedPlan = StringUtils.abbreviate(plan, properties.getMaxplanlength());
		Deque<ConversationTurn> deque = history.computeIfAbsent(threadId, k -> new ArrayDeque<>());
		synchronized (deque) {
			// 保持历史记录窗口大小，移除最旧的记录
			while (deque.size() >= properties.getMaxturnhistory()) {
				deque.pollFirst();
			}
			deque.addLast(new ConversationTurn(pending.userQuestion, trimmedPlan));
		}
	}

	/**
	 * 丢弃当前挂起的轮次数据
	 * <p>
	 * 当请求被异常中断或取消时调用。该方法仅清除暂存区 (`pendingTurns`) 的数据，
	 * 不会影响已经归档的历史记录 (`history`)，防止存入不完整或脏数据。
	 * </p>
	 * 
	 * @param threadId 会话线程 ID
	 */
	public void discardPending(String threadId) {
		pendingTurns.remove(threadId);
	}

	/**
	 * 重启最后一轮对话 (通常用于人工反馈场景)
	 * <p>
	 * 当用户拒绝了 AI 的计划并提供反馈时，我们需要“撤回”上一次的归档，让 AI 重新生成计划。
	 * 该方法会将 `history` 中最后一条记录取出，并将其问题部分重新放入 `pendingTurns`，
	 * 准备接收新的 Planner 输出。
	 * </p>
	 * 
	 * @param threadId 会话线程 ID
	 */
	public void restartLastTurn(String threadId) {
		Deque<ConversationTurn> deque = history.get(threadId);
		if (deque == null || deque.isEmpty()) {
			return;
		}
		ConversationTurn lastTurn;
		synchronized (deque) {
			lastTurn = deque.pollLast();
		}
		if (lastTurn != null) {
			pendingTurns.put(threadId, new PendingTurn(lastTurn.userQuestion()));
		}
	}

	/**
	 * 构建多轮对话上下文文本
	 * <p>
	 * 将历史记录格式化为字符串，以便注入到 LLM 的 Prompt 中。
	 * 格式示例：
	 * 用户: ...
	 * AI计划: ...
	 * </p>
	 * 
	 * @param threadId 会话线程 ID
	 * @return 格式化后的历史对话文本
	 */
	public String buildContext(String threadId) {
		Deque<ConversationTurn> deque = history.get(threadId);
		if (deque == null || deque.isEmpty()) {
			return "(无)";
		}
		return deque.stream()
				.map(turn -> "用户: " + turn.userQuestion() + "\nAI计划: " + turn.plan())
				.collect(Collectors.joining("\n"));
	}

	private record ConversationTurn(String userQuestion, String plan) {
	}

	private static class PendingTurn {

		private final String userQuestion;

		private final StringBuilder planBuilder = new StringBuilder();

		private PendingTurn(String userQuestion) {
			this.userQuestion = userQuestion;
		}

	}

}
