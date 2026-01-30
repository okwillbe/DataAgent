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
package com.alibaba.cloud.ai.dataagent.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class GraphRequest {

	/**
	 * Agent Id
	 */
	private String agentId;

	/**
	 * 线程ID
	 */
	private String threadId;

	/**
	 * 查询内容
	 */
	private String query;

	/**
	 * 是否需要人工审核
	 */
	private boolean humanFeedback;

	/**
	 * 人工审核内容
	 */
	private String humanFeedbackContent;

	/**
	 * 是否拒绝计划
	 */
	private boolean rejectedPlan;

	/**
	 * 是否仅NL2SQL
	 */
	private boolean nl2sqlOnly;

}
