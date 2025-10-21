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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.KNIMEException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.PortTypeRegistry;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContainer;
import org.knime.core.node.workflow.NodeID;
import org.knime.core.node.workflow.NodeID.NodeIDSuffix;
import org.knime.core.node.workflow.SubNodeContainer;
import org.knime.core.node.workflow.WorkflowAnnotationID;
import org.knime.core.node.workflow.WorkflowCopyContent;
import org.knime.core.node.workflow.WorkflowManager;
import org.knime.core.node.workflow.capture.IsolatedExecutor.WorkflowSegmentNodeMessage;
import org.knime.core.node.workflow.capture.WorkflowSegmentExecutor.BuilderParams;
import org.knime.core.node.workflow.virtual.DefaultVirtualPortObjectInNodeFactory;
import org.knime.core.node.workflow.virtual.DefaultVirtualPortObjectInNodeModel;
import org.knime.core.node.workflow.virtual.DefaultVirtualPortObjectOutNodeFactory;
import org.knime.core.node.workflow.virtual.DefaultVirtualPortObjectOutNodeModel;
import org.knime.core.node.workflow.virtual.VirtualNodeContext.Restriction;
import org.knime.core.node.workflow.virtual.VirtualNodeInput;
import org.knime.core.node.workflow.virtual.parchunk.FlowVirtualScopeContext;
import org.knime.core.util.Pair;

import jakarta.json.JsonException;
import jakarta.json.JsonValue;

/**
 * TODO
 *
 * @author Martin Horn, KNIME GmbH, Konstanz, Germany
 *
 * @since 5.8
 * @noreference This class is not intended to be referenced by clients.
 */
public final class CombinedExecutor {

    /**
     * TODO
     */
    public static final class Builder {

        final BuilderParams m_params;

        final PortObject[] m_initialInputs;

        WorkflowManager m_combinedWorkflow;

        Builder(final BuilderParams params, final PortObject[] initialInputs) {
            m_params = params;
            m_initialInputs = initialInputs;
            m_combinedWorkflow = null;
        }

        Builder(final BuilderParams params, final WorkflowManager combinedWorkflow) {
            m_params = params;
            m_combinedWorkflow = combinedWorkflow;
            m_initialInputs = null;
        }

        /**
         * @return TODO
         */
        public CombinedExecutor build() {
            return new CombinedExecutor(this);
        }
    }

    /**
     * TODO
     *
     * @param nodeIDSuffix
     * @param portIndex
     */
    public record PortId(NodeIDSuffix nodeIDSuffix, int portIndex) {

    }

    /**
     * TODO
     *
     * @param outputs
     * @param flowVariables
     * @param nodeMessages
     * @param ids TODO naming
     * @param component
     */
    public record WorkflowSegmentExecutionResult(PortObject[] outputs, List<FlowVariable> flowVariables,
        List<WorkflowSegmentNodeMessage> nodeMessages, String[] ids, SubNodeContainer component) {
    }

    private WorkflowManager m_hostWfm;

    private NativeNodeContainer m_hostNode;

    private CombinedExecutor(final Builder builder) {
        m_hostNode = builder.m_params.hostNode();
        if (builder.m_combinedWorkflow == null) {
            assert builder.m_initialInputs != null;
            var projWfm = m_hostNode.getParent().getProjectWFM();
            try {
                // TODO proper workflow name?
                m_hostWfm = WorkflowSegmentExecutor.createTemporaryWorkflowProject(projWfm.getWorkflowDataRepository(),
                    projWfm.getContextV2());
            } catch (KNIMEException ex) {
                // TODO
                throw new RuntimeException(ex);
            }
            PortType[] inTypes = Stream.of(builder.m_initialInputs)
                .map(i -> PortTypeRegistry.getInstance().getPortType(i.getClass())).toArray(PortType[]::new);
            var virtualInId = m_hostWfm.createAndAddNode(new DefaultVirtualPortObjectInNodeFactory(inTypes));
            ((DefaultVirtualPortObjectInNodeModel)((NativeNodeContainer)m_hostWfm.getNodeContainer(virtualInId))
                .getNodeModel())
                    .setVirtualNodeInput(new VirtualNodeInput(builder.m_initialInputs, Collections.emptyList()));
        } else {
            m_hostWfm = builder.m_combinedWorkflow;
        }
    }

    /**
     * TODO
     *
     * @return
     */
    public WorkflowManager getWorkflow() {
        return m_hostWfm;
    }

    /**
     * @param ws
     * @param inputs
     * @param parameters a map of parameter names to the new to be set configuration value as json. Sets the
     *            configuration of the (config) nodes referenced by the given parameter name. Only considers nodes on
     *            the top level (cp. {@link WorkflowManager#setConfigurationNodes(Map)}). Can be {@code null}.
     * @param dataAreaPath absolute path to the workflow's data area or {@code null} if none
     * @param restrictions restrictions on the execution of the workflow segment
     * @return TODO
     */
    public WorkflowSegmentExecutionResult execute(final WorkflowSegment ws, final List<PortId> inputs,
        final Map<String, JsonValue> parameters, final Path dataAreaPath, final Restriction... restrictions) {
        var wfm = ws.loadWorkflow();

        var orgNodeIds = wfm.getNodeContainers().stream().map(NodeContainer::getID).toArray(NodeID[]::new);
        var persistor = wfm.copy(WorkflowCopyContent.builder().setNodeIDs(orgNodeIds)
            .setAnnotationIDs(wfm.getWorkflowAnnotationIDs().toArray(WorkflowAnnotationID[]::new)).build());
        var copyContent = m_hostWfm.paste(persistor);
        var nodeIdMapping = new HashMap<NodeIDSuffix, NodeID>();
        for (int i = 0; i < orgNodeIds.length; i++) {
            nodeIdMapping.put(NodeIDSuffix.create(wfm.getID(), orgNodeIds[i]), copyContent.getNodeIDs()[i]);
        }

        // connect inputs
        var wsInputs = ws.getConnectedInputs();
        assert wsInputs.size() == inputs.size();
        for (int i = 0; i < inputs.size(); i++) {
            for (var portId : wsInputs.get(i).getConnectedPorts()) {
                var nodeId = nodeIdMapping.get(portId.getNodeIDSuffix());
                m_hostWfm.addConnection(toNodeID(inputs.get(i)), inputs.get(i).portIndex(), nodeId, portId.getIndex());
            }
        }

        // connect outputs
        List<PortType> outTypes = new ArrayList<>();
        List<Pair<NodeID, Integer>> outPorts = new ArrayList<>();
        for (var outputNodeId : m_hostWfm.findNodes(DefaultVirtualPortObjectOutNodeModel.class, false).keySet()) {
            // collect outports that are already connected to the output node
            m_hostWfm.getIncomingConnectionsFor(outputNodeId).forEach(cc -> {
                outTypes.add(m_hostWfm.getNodeContainer(cc.getSource()).getOutPort(cc.getSourcePort()).getPortType());
                outPorts.add(Pair.create(cc.getSource(), cc.getSourcePort()));
            });
            m_hostWfm.removeNode(outputNodeId);
        }
        var wsOutputs = ws.getConnectedOutputs();
        wsOutputs.stream().filter(o -> o.getConnectedPort().isPresent() && o.getType().isPresent()).forEach(o -> {
            outTypes.add(o.getType().orElseThrow());
            var portId = o.getConnectedPort();
            var nodeId = nodeIdMapping.get(portId.get().getNodeIDSuffix());
            outPorts.add(Pair.create(nodeId, portId.get().getIndex()));
        });
        var outputNodeId =
            m_hostWfm.createAndAddNode(new DefaultVirtualPortObjectOutNodeFactory(outTypes.toArray(PortType[]::new)));
        for (int i = 0; i < outPorts.size(); i++) {
            m_hostWfm.addConnection(outPorts.get(i).getFirst(), outPorts.get(i).getSecond(), outputNodeId, i + 1);
        }

        // collapse into component
        var componentId = m_hostWfm.convertMetaNodeToSubNode(
            m_hostWfm.collapseIntoMetaNode(copyContent.getNodeIDs(), copyContent.getAnnotationIDs(), ws.getName())
                .getCollapsedMetanodeID())
            .getConvertedNodeID();

        // configuration nodes
        var component = (SubNodeContainer)m_hostWfm.getNodeContainer(componentId);
        if (parameters != null && !parameters.isEmpty()) {
            try {
                component.getWorkflowManager().setConfigurationNodes(parameters);
            } catch (JsonException | InvalidSettingsException ex) {
                // TODO
                throw new RuntimeException(ex);
            }
        }

        var flowVirtualScopeContext = new FlowVirtualScopeContext(m_hostNode.getID(), dataAreaPath, restrictions);
        component.getWorkflowManager().setInitialScopeContext(flowVirtualScopeContext);

        // TODO layouting

        // execute and extract tool outputs
        m_hostWfm.executeAllAndWaitUntilDone();
        var outputs = m_hostWfm.getIncomingConnectionsFor(outputNodeId).stream() //
            .filter(cc -> cc.getSource().equals(componentId)) //
            .map(cc -> component.getOutPort(cc.getSourcePort()).getPortObject()) //
            .toArray(PortObject[]::new);
        var ids = m_hostWfm.getIncomingConnectionsFor(outputNodeId).stream() //
            .filter(cc -> cc.getSource().equals(componentId)) //
            .map(cc -> NodeIDSuffix.create(m_hostWfm.getID(), cc.getSource()) + "#" + cc.getSourcePort()) //
            .toArray(String[]::new);
        // TODO flow variables and collect error/warning messages
        return new WorkflowSegmentExecutionResult(outputs, List.of(), List.of(), ids, component);
    }

    private NodeID toNodeID(final PortId portId) {
        return portId.nodeIDSuffix().prependParent(m_hostWfm.getID());
    }

    /**
     * TODO
     */
    public void dispose() {
        // TODO
    }

}
