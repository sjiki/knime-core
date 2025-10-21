/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Oct 20, 2025 (hornm): created
 */
package org.knime.core.node.workflow.capture;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.KNIMEException;
import org.knime.core.node.Node;
import org.knime.core.node.exec.dataexchange.PortObjectRepository;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.PortTypeRegistry;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContainer;
import org.knime.core.node.workflow.NodeID;
import org.knime.core.node.workflow.NodeMessage;
import org.knime.core.node.workflow.NodeMessage.Type;
import org.knime.core.node.workflow.NodeUIInformation;
import org.knime.core.node.workflow.SubNodeContainer;
import org.knime.core.node.workflow.WorkflowCopyContent;
import org.knime.core.node.workflow.WorkflowManager;
import org.knime.core.node.workflow.capture.WorkflowSegment.Input;
import org.knime.core.node.workflow.capture.WorkflowSegment.Output;
import org.knime.core.node.workflow.capture.WorkflowSegment.PortID;
import org.knime.core.node.workflow.capture.WorkflowSegmentExecutor.BuilderParams;
import org.knime.core.node.workflow.capture.WorkflowSegmentExecutor.ExecutionMode;
import org.knime.core.node.workflow.virtual.DefaultVirtualPortObjectInNodeFactory;
import org.knime.core.node.workflow.virtual.DefaultVirtualPortObjectInNodeModel;
import org.knime.core.node.workflow.virtual.DefaultVirtualPortObjectOutNodeFactory;
import org.knime.core.node.workflow.virtual.DefaultVirtualPortObjectOutNodeModel;
import org.knime.core.node.workflow.virtual.VirtualNodeContext.Restriction;
import org.knime.core.node.workflow.virtual.VirtualNodeInput;
import org.knime.core.node.workflow.virtual.parchunk.FlowVirtualScopeContext;
import org.knime.core.util.Pair;

import jakarta.json.JsonValue;

/**
 * TODO
 *
 * @author Martin Horn, KNIME GmbH, Konstanz, Germany
 *
 * @since 5.8
 * @noreference This class is not intended to be referenced by clients.
 */
public final class IsolatedExecutor {

    /**
     * TODO
     */
    public static final class Builder {

        final BuilderParams m_params;

        boolean m_executeAll;

        Builder(final BuilderParams params, final boolean executeAll) {
            m_params = params;
            m_executeAll = executeAll;
        }

        /**
         * @return TODO
         */
        public IsolatedExecutor build() {
            return new IsolatedExecutor(this);
        }

    }

    /**
     * Represents the result of the executed workflow including a hierarchical list of the error and warning messages of
     * all nodes.
     *
     * @param outputs the resulting port objects or {@code null} if the execution failed
     * @param flowVariables the resulting flow variables
     * @param nodeMessages a hierarchical list of the error and warning messages of all nodes
     */
    public record WorkflowSegmentExecutionResult(PortObject[] outputs, List<FlowVariable> flowVariables,
        List<WorkflowSegmentNodeMessage> nodeMessages) {
        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            WorkflowSegmentExecutionResult result = (WorkflowSegmentExecutionResult)obj;
            return Arrays.equals(outputs, result.outputs)
                && Objects.equals(flowVariables, result.flowVariables)
                && Objects.equals(nodeMessages, result.nodeMessages);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((outputs == null) ? 0 : Arrays.hashCode(outputs));
            result = prime * result + ((flowVariables == null) ? 0 : flowVariables.hashCode());
            result = prime * result + ((nodeMessages == null) ? 0 : nodeMessages.hashCode());
            return result;
        }

        @Override
        public String toString() {
            return "WorkflowSegmentExecutionResult{" + "portObjectCopies=" + Arrays.toString(outputs)
                + ", flowVariables=" + flowVariables + ", nodeMessages=" + nodeMessages + '}';
        }

        /**
         * Helper to aggregate a single error message based on the node error message in execution result.
         *
         * @return the compiled error message
         *
         * @since 5.5
         */
        public String compileSingleErrorMessage() {
            var errorMessages = nodeMessages.stream().toList();
            // determine the number of failed nodes that are not containers
            List<WorkflowSegmentNodeMessage> leafErrorMessages = new ArrayList<>();
            for (WorkflowSegmentNodeMessage message : errorMessages) {
                recursivelyExtractLeafNodeErrorMessages(message, leafErrorMessages);
            }
            return constructErrorMessage(leafErrorMessages.size(), errorMessages);
        }

        private static void recursivelyExtractLeafNodeErrorMessages(final WorkflowSegmentNodeMessage message,
            final List<WorkflowSegmentNodeMessage> result) {
            if (message.message().getMessageType() == Type.RESET) {
                return;
            }
            if (message.recursiveMessages().isEmpty()) {
                result.add(message);
            } else {
                for (WorkflowSegmentNodeMessage nestedMessage : message.recursiveMessages()) {
                    recursivelyExtractLeafNodeErrorMessages(nestedMessage, result);
                }
            }
        }

        private static String constructErrorMessage(final int numberOfFailedNodes,
            final List<WorkflowSegmentNodeMessage> errorMessages) {
            String iNodes;
            if (numberOfFailedNodes == 1) {
                iNodes = "one node";
            } else {
                iNodes = String.valueOf(numberOfFailedNodes) + " nodes";
            }
            return String.format("Workflow contains %s with execution failure:%n%s", iNodes, errorMessages.stream()//
                .map(msg -> recursivelyConstructErrorMessage(msg, ""))//
                .collect(Collectors.joining(",\n")));
        }

        private static String recursivelyConstructErrorMessage(final WorkflowSegmentNodeMessage message,
            final String prefix) {
            if (message.recursiveMessages().isEmpty()) {
                return prefix + message.nodeName() + " #" + message.nodeID().getIndex() + ": "
                    + removeErrorPrefix(message.message().getMessage());
            } else {
                String newPrefix = prefix + message.nodeName() + " #" + message.nodeID().getIndex() + " > ";
                return message.recursiveMessages().stream().filter(msg -> msg.message().getMessageType() == Type.ERROR)
                    .map(ms -> recursivelyConstructErrorMessage(ms, newPrefix)).collect(Collectors.joining(",\n"));
                // WorkflowSegmentNodeMessage of type ERROR can contain messages of type WARNING
            }
        }

        private static String removeErrorPrefix(final String msg) {
            if (msg.startsWith(Node.EXECUTE_FAILED_PREFIX)) {
                return StringUtils.removeStart(msg, Node.EXECUTE_FAILED_PREFIX);
            }
            return msg;
        }
    }

    private final Builder m_builder;

    private WorkflowManager m_hostWfm;

    private boolean m_shallDisposeHostWfm;

    private WorkflowManager m_wfm;

    private IsolatedExecutor(final Builder builder) {
        m_builder = builder;
    }

    /**
     * Represents messages of nodes containing name, node ID, error or warning message and WorkflowSegmentNodeMessages
     * of nested nodes.
     *
     * @param nodeName the node name
     * @param nodeID the node ID
     * @param message the error message of the node
     * @param recursiveMessages a list of node messages for nested nodes if the node is a container, otherwise an empty
     *            list
     *
     * @since 5.4
     */
    public record WorkflowSegmentNodeMessage(String nodeName, NodeID nodeID, NodeMessage message,
        List<WorkflowSegmentNodeMessage> recursiveMessages) {
    }

    /**
     * TODO
     *
     * @return TODO what workflow exactly? Null?
     */
    public WorkflowManager getWorkflow() {
        return null;
    }

    /**
     * TODO
     *
     * @param ws the workflow segment to execute
     * @param inputData the input data to be used for execution
     * @param parameters a map of parameter names to the new to be set configuration value as json. Sets the
     *            configuration of the (config) nodes referenced by the given parameter name. Only considers nodes on
     *            the top level (cp. {@link WorkflowManager#setConfigurationNodes(Map)}). Can be {@code null}.
     * @param dataAreaPath absolute path to the workflow's data area or {@code null} if none
     * @param restrictions restrictions on the execution of the workflow segment
     * @return TODO
     * @throws Exception TODO
     *
     */
    public WorkflowSegmentExecutionResult execute(final WorkflowSegment ws, final PortObject[] inputData,
        final Map<String, JsonValue> parameters, final Path dataAreaPath, final Restriction... restrictions)
        throws Exception {
        var hostNode = m_builder.m_params.hostNode();
        var mode = m_builder.m_params.mode();
        if (mode == ExecutionMode.DETACHED) {
            var projWfm = hostNode.getParent().getProjectWFM();
            m_hostWfm = WorkflowSegmentExecutor.createTemporaryWorkflowProject(projWfm.getWorkflowDataRepository(),
                projWfm.getContextV2());
            m_shallDisposeHostWfm = true;
        } else {
            m_hostWfm = hostNode.getParent();
            m_shallDisposeHostWfm = false;
        }

        try (var unused = m_hostWfm.lock()) {
            if (mode != ExecutionMode.DETACHED && !m_hostWfm.canModifyStructure()) {
                throw new KNIMEException(
                    "Cannot execute workflow segment in %s mode because it's part of an already executed component."
                        .formatted(mode.name().toLowerCase(Locale.ENGLISH)));
            }
            m_wfm =
                m_hostWfm.createAndAddSubWorkflow(new PortType[0], new PortType[0], m_builder.m_params.workflowName());
        }

        var flowVirtualScopeContext = new FlowVirtualScopeContext(hostNode.getID(), dataAreaPath, restrictions);
        m_wfm.setInitialScopeContext(flowVirtualScopeContext);
        if (mode != ExecutionMode.DEBUG) {
            m_wfm.hideInUI();
        }

        // position
        NodeUIInformation startUIPlain = hostNode.getUIInformation();
        if (startUIPlain != null) {
            NodeUIInformation startUI =
                NodeUIInformation.builder(startUIPlain).translate(new int[]{60, -60, 0, 0}).build();
            m_wfm.setUIInformation(startUI);
        }

        // copy workflow segment into metanode
        WorkflowManager segmentWorkflow = BuildWorkflowsUtil.loadWorkflow(ws, m_builder.m_params.loadWarningConsumer());
        NodeID[] ids = segmentWorkflow.getNodeContainers().stream().map(NodeContainer::getID).toArray(NodeID[]::new);
        m_wfm.copyFromAndPasteHere(segmentWorkflow, WorkflowCopyContent.builder().setNodeIDs(ids).build());
        // TODO?
        // ws.disposeWorkflow();
        var virtualIONodes = addVirtualIONodes(ws, m_wfm);

        if (parameters != null && !parameters.isEmpty()) {
            m_wfm.setConfigurationNodes(parameters);
        }

        NativeNodeContainer virtualInNode = ((NativeNodeContainer)m_wfm.getNodeContainer(virtualIONodes.startId()));
        DefaultVirtualPortObjectInNodeModel inNM = (DefaultVirtualPortObjectInNodeModel)virtualInNode.getNodeModel();

        var exec = m_builder.m_params.exec();
        flowVirtualScopeContext.registerHostNode(hostNode, exec);

        inNM.setVirtualNodeInput(new VirtualNodeInput(inputData,
            WorkflowSegmentExecutor.collectOutputFlowVariablesFromUpstreamNodes(hostNode)));
        NativeNodeContainer nnc = (NativeNodeContainer)m_wfm.getNodeContainer(virtualIONodes.endId());

        AtomicReference<Exception> exception = new AtomicReference<>();
        WorkflowSegmentExecutor.executeAndWait(hostNode, m_wfm, exec,
            m_builder.m_executeAll ? null : virtualIONodes.endId, exception);

        if (exception.get() != null) {
            throw exception.get();
        }

        DefaultVirtualPortObjectOutNodeModel outNM = (DefaultVirtualPortObjectOutNodeModel)nnc.getNodeModel();
        PortObject[] portObjectCopies = copyPortObjects(outNM.getOutObjects(), exec);
        // if (portObjectCopies != null) {
        //     removeSuperfluousFileStores(Stream.concat(stream(portObjectCopies), outputData.stream()));
        // }

        if (m_builder.m_params.collectMessages()) {
            boolean executionSuccessful = m_wfm.getNodeContainerState().isExecuted();
            return collectNodeMessagesAndCreateResult(executionSuccessful ? portObjectCopies : null,
                WorkflowSegmentExecutor.getFlowVariablesFromNC(nnc), m_wfm);
        } else {
            return new WorkflowSegmentExecutionResult(portObjectCopies,
                WorkflowSegmentExecutor.getFlowVariablesFromNC(nnc), null);
        }

    }

    /**
     * TODO update
     *
     * Executes the workflow segment and additionally returns a hierarchical list containing the error and warning
     * messages of all nodes. Reset messages are filtered out if they don't contain nested error or warning messages.
     * The method is used in
     * org.knime.python3.nodes/src/main/java/org/knime/python3/nodes/ports/PythonWorkflowPortObject.java to construct an
     * exception message for python nodes.
     *
     * @param inputData the input data to be used for execution
     * @param exec for cancellation
     * @return the resulting port objects, flow variables and a hierarchical list of node error and warning messages
     * @throws Exception if workflow execution fails
     * @throws IllegalStateException if the underlying workflow has been disposed already
     *
     * @since 5.4
     */
    private static WorkflowSegmentExecutionResult collectNodeMessagesAndCreateResult(final PortObject[] outputs,
        final List<FlowVariable> flowVariables, final WorkflowManager wfm) {
        return new WorkflowSegmentExecutionResult(outputs, flowVariables, recursivelyExtractNodeMessages(wfm));
    }

    private static VirtualIONodes addVirtualIONodes(final WorkflowSegment wf, final WorkflowManager wfm) {

        //add virtual in node
        List<Input> inputs = wf.getConnectedInputs();
        PortType[] inTypes =
            inputs.stream().map(i -> getNonOptionalType(i.getType().get())).toArray(s -> new PortType[s]);
        int[] wfBounds = NodeUIInformation.getBoundingBoxOf(wfm.getNodeContainers());
        var virtualStartID = wfm.createAndAddNode(new DefaultVirtualPortObjectInNodeFactory(inTypes));
        Pair<Integer, int[]> pos = BuildWorkflowsUtil.getInputOutputNodePositions(wfBounds, 1, true);
        wfm.getNodeContainer(virtualStartID).setUIInformation(
            NodeUIInformation.builder().setNodeLocation(pos.getFirst(), pos.getSecond()[0], -1, -1).build());

        //add virtual out node
        List<Output> outputs = wf.getConnectedOutputs();
        PortType[] outTypes =
            outputs.stream().map(o -> getNonOptionalType(o.getType().get())).toArray(s -> new PortType[s]);
        var virtualEndID = wfm.createAndAddNode(new DefaultVirtualPortObjectOutNodeFactory(outTypes));
        pos = BuildWorkflowsUtil.getInputOutputNodePositions(wfBounds, 1, false);
        wfm.getNodeContainer(virtualEndID).setUIInformation(
            NodeUIInformation.builder().setNodeLocation(pos.getFirst(), pos.getSecond()[0], -1, -1).build());

        //connect virtual in
        for (int i = 0; i < inputs.size(); i++) {
            for (PortID p : inputs.get(i).getConnectedPorts()) {
                wfm.addConnection(virtualStartID, i + 1, p.getNodeIDSuffix().prependParent(wfm.getID()), p.getIndex());
            }
        }

        //connect virtual out
        for (int i = 0; i < outputs.size(); i++) {
            PortID p = outputs.get(i).getConnectedPort().orElse(null);
            if (p != null) {
                wfm.addConnection(p.getNodeIDSuffix().prependParent(wfm.getID()), p.getIndex(), virtualEndID, i + 1);
            }
        }

        return new VirtualIONodes(virtualStartID, virtualEndID);
    }

    private static PortType getNonOptionalType(final PortType p) {
        return PortTypeRegistry.getInstance().getPortType(p.getPortObjectClass());
    }

    private static List<WorkflowSegmentNodeMessage> recursivelyExtractNodeMessages(final NodeContainer nc) {
        if (nc instanceof NativeNodeContainer) {
            return Collections.emptyList();
        }
        WorkflowManager wfm = null;
        if (nc instanceof WorkflowManager w) {
            wfm = w;
        } else if (nc instanceof SubNodeContainer snc) {
            wfm = snc.getWorkflowManager();
        } else {
            throw new IllegalStateException("Received unexpected NodeContainer.");
        }
        return wfm.getNodeContainers().stream()
            .map(n -> new WorkflowSegmentNodeMessage(n.getName(), n.getID(), n.getNodeMessage(),
                recursivelyExtractNodeMessages(n)))
            .filter(msg -> (!msg.recursiveMessages().isEmpty()) || (msg.message().getMessageType() == Type.ERROR)
                || (msg.message().getMessageType() == Type.WARNING))
            .toList();
    }

    private static PortObject[] copyPortObjects(final PortObject[] portObjects, final ExecutionContext exec)
        throws IOException, CanceledExecutionException {
        if (portObjects == null) {
            return null; // NOSONAR
        }
        PortObject[] portObjectCopies = new PortObject[portObjects.length];
        for (int i = 0; i < portObjects.length; i++) {
            if (portObjects[i] != null) {
                portObjectCopies[i] = PortObjectRepository.copy(portObjects[i], exec, exec);
            }
        }
        return portObjectCopies;
    }

    /**
     * Cancels the execution if it is running and removes the virtual node containing the workflow segment from the
     * hosting workflow.
     */
    public void dispose() {
        cancel();
        m_wfm.getParent().removeNode(m_wfm.getID());
        m_wfm = null;
        if (m_shallDisposeHostWfm) {
            WorkflowManager.ROOT.removeProject(m_hostWfm.getID());
        }
        m_hostWfm = null;
    }

    /**
     * Cancels the execution of the workflow segment.
     *
     * @throws IllegalStateException if the underlying workflow has been disposed already
     */
    public void cancel() {
        checkWfmNonNull();
        WorkflowSegmentExecutor.cancel(m_wfm);
    }

    private void checkWfmNonNull() {
        if (m_wfm == null) {
            throw new IllegalStateException(
                "Can't extract error messages from workflow segment. Workflow has been disposed already.");
        }
    }

    private record VirtualIONodes(NodeID startId, NodeID endId) {

    }

}
