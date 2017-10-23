package org.apache.nifi.atlas.emulator;

import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.NiFiTypes;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.util.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Emulate Atlas API v2 server for NiFi implementation testing.
 */
public class AtlasAPIV2ServerEmulator {

    private static final Logger logger = LoggerFactory.getLogger(AtlasAPIV2ServerEmulator.class);
    private Server server;
    private ServerConnector httpConnector;
    private AtlasNotificationServerEmulator notificationServerEmulator;

    public static void main(String[] args) throws Exception {
        final AtlasAPIV2ServerEmulator emulator = new AtlasAPIV2ServerEmulator();
        emulator.start();
    }

    public void start() throws Exception {
        if (server == null) {
            createServer();
        }

        server.start();
        logger.info("Starting {} on port {}", AtlasAPIV2ServerEmulator.class.getSimpleName(), httpConnector.getLocalPort());

        notificationServerEmulator.consume(m -> {
            if (m instanceof HookNotification.EntityCreateRequest) {
                HookNotification.EntityCreateRequest em = (HookNotification.EntityCreateRequest) m;
                for (Referenceable ref : em.getEntities()) {
                    final AtlasEntity entity = toEntity(ref);
                    updateEntityByNotification(entity);
                }
            } else if (m instanceof HookNotification.EntityPartialUpdateRequest) {
                HookNotification.EntityPartialUpdateRequest em
                        = (HookNotification.EntityPartialUpdateRequest) m;
                final AtlasEntity entity = toEntity(em.getEntity());
                updateEntityByNotification(entity);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void updateEntityByNotification(AtlasEntity entity) {
        final String key = toEntityKey(entity);
        final AtlasEntity exEntity = atlasEntities.get(key);

        if (exEntity != null) {
            convertReferenceableToObjectId(entity.getAttributes()).forEach((k, v) -> {
                Object r = v;
                final Object exAttr = exEntity.getAttribute(k);
                if (exAttr != null && exAttr instanceof Collection) {
                    ((Collection) exAttr).addAll((Collection) v);
                    r = exAttr;
                }
                exEntity.setAttribute(k, r);
            });
        } else {
            atlasEntities.put(key, entity);
        }
    }

    private void createServer() throws Exception {
        server = new Server();
        final ContextHandlerCollection handlerCollection = new ContextHandlerCollection();

        final ServletContextHandler staticContext = new ServletContextHandler();
        staticContext.setContextPath("/");

        final ServletContextHandler atlasApiV2Context = new ServletContextHandler();
        atlasApiV2Context.setContextPath("/api/atlas/v2/");

        handlerCollection.setHandlers(new Handler[]{staticContext, atlasApiV2Context});

        server.setHandler(handlerCollection);

        final ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setBaseResource(Resource.newClassPathResource("public", false, false));
        staticContext.setHandler(resourceHandler);

        final ServletHandler servletHandler = new ServletHandler();
        atlasApiV2Context.insertHandler(servletHandler);

        httpConnector = new ServerConnector(server);
        httpConnector.setPort(21000);

        server.setConnectors(new Connector[] {httpConnector});

        servletHandler.addServletWithMapping(TypeDefsServlet.class, "/types/typedefs/");
        servletHandler.addServletWithMapping(EntityBulkServlet.class, "/entity/bulk/");
        servletHandler.addServletWithMapping(EntitySearchServlet.class, "/search/basic/");
        servletHandler.addServletWithMapping(LineageServlet.class, "/debug/lineage/");

        notificationServerEmulator = new AtlasNotificationServerEmulator();
    }

    public void stop() throws Exception {
        notificationServerEmulator.stop();
        server.stop();
    }

    private static void respondWithJson(HttpServletResponse resp, Object entity) throws IOException {
        respondWithJson(resp, entity, HttpServletResponse.SC_OK);
    }

    private static void respondWithJson(HttpServletResponse resp, Object entity, int statusCode) throws IOException {
        resp.setContentType("application/json");
        resp.setStatus(statusCode);
        final ServletOutputStream out = resp.getOutputStream();
        new ObjectMapper().writer().writeValue(out, entity);
        out.flush();
    }

    private static <T> T readInputJSON(HttpServletRequest req, Class<? extends T> clazz) throws IOException {
        return new ObjectMapper().reader().withType(clazz).readValue(req.getInputStream());
    }

    private static final AtlasTypesDef atlasTypesDef = new AtlasTypesDef();
    // key = type::qualifiedName
    private static final Map<String, AtlasEntity> atlasEntities = new HashMap<>();

    public static class TypeDefsServlet extends HttpServlet {

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            final String name = req.getParameter("name");
            AtlasTypesDef result = atlasTypesDef;
            if (name != null && !name.isEmpty()) {
                result = new AtlasTypesDef();
                final Optional<AtlasEntityDef> entityDef = atlasTypesDef.getEntityDefs().stream().filter(en -> en.getName().equals(name)).findFirst();
                if (entityDef.isPresent()) {
                    result.getEntityDefs().add(entityDef.get());
                }
            }
            respondWithJson(resp, result);
        }

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

            final AtlasTypesDef newTypes = readInputJSON(req, AtlasTypesDef.class);
            final Map<String, AtlasEntityDef> defs = new HashMap<>();
            for (AtlasEntityDef existingDef : atlasTypesDef.getEntityDefs()) {
                defs.put(existingDef.getName(), existingDef);
            }
            for (AtlasEntityDef entityDef : newTypes.getEntityDefs()) {
                defs.put(entityDef.getName(), entityDef);
            }
            atlasTypesDef.setEntityDefs(defs.values().stream().collect(Collectors.toList()));

            respondWithJson(resp, atlasTypesDef);
        }

        @Override
        protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            super.doPut(req, resp);
        }
    }

    public static class EntityBulkServlet extends HttpServlet {

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            final AtlasEntity.AtlasEntitiesWithExtInfo withExtInfo = readInputJSON(req, AtlasEntity.AtlasEntitiesWithExtInfo.class);
            withExtInfo.getEntities().forEach(entity -> atlasEntities.put(toEntityKey((AtlasEntity) entity), entity));
            final EntityMutationResponse mutationResponse = new EntityMutationResponse();
            respondWithJson(resp, mutationResponse);
        }

        @Override
        protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            atlasEntities.clear();
            resp.setStatus(200);
        }
    }

    public static class EntitySearchServlet extends HttpServlet {

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            final AtlasSearchResult result = new AtlasSearchResult();
            result.setEntities(atlasEntities.values().stream()
                    .map(entity -> new AtlasEntityHeader(entity.getTypeName(), entity.getAttributes())).collect(Collectors.toList()));
            respondWithJson(resp, result);
        }
    }

    private static AtlasEntity toEntity(Referenceable ref) {
        return new AtlasEntity(ref.getTypeName(), convertReferenceableToObjectId(ref.getValuesMap()));
    }

    private static Map<String, Object> convertReferenceableToObjectId(Map<String, Object> values) {
        final Map<String, Object> result = new HashMap<>();
        for (String k : values.keySet()) {
            Object v = values.get(k);
            result.put(k, toV2(v));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Object toV2(Object v) {
        Object r = v;
        if (v instanceof Referenceable) {
            r = toMap((Referenceable) v);
        } else if (v instanceof Map) {
            r = convertReferenceableToObjectId((Map<String, Object>) v);
        } else if (v instanceof Collection) {
            r = ((Collection) v).stream().map(c -> toV2(c)).collect(Collectors.toList());
        }
        return r;
    }

    private static Map<String, Object> toMap(Referenceable ref) {
        final HashMap<String, Object> result = new HashMap<>();
        result.put("typeName", ref.getTypeName());
        final HashMap<String, String> uniqueAttrs = new HashMap<>();
        uniqueAttrs.put("qualifiedName", (String) ref.getValuesMap().get("qualifiedName"));
        result.put("uniqueAttributes", uniqueAttrs);
        return result;
    }

    private static String toEntityKey(AtlasEntity entity) {
        return toEntityKey(entity.getTypeName(), (String) entity.getAttribute("qualifiedName"));
    }

    private static String toEntityKey(String typeName, String qName) {
        return typeName + "::" + qName;
    }

    public static class LineageServlet extends HttpServlet {

        private Node toNode(AtlasEntity entity) {
            Node node = new Node();
            node.setName(entity.getAttribute(NiFiTypes.ATTR_NAME).toString());
            node.setQualifiedName(entity.getAttribute(NiFiTypes.ATTR_QUALIFIED_NAME).toString());
            node.setType(entity.getTypeName());
            return node;
        }

        private Link toLink(AtlasEntity s, AtlasEntity t, Map<String, Integer> nodeIndices) {
            final Integer sid = nodeIndices.get(toEntityKey(s));
            final Integer tid = nodeIndices.get(toEntityKey(t));

            return new Link(sid, tid);
        }

        private String toLinkKey(Integer s, Integer t) {
            return s + "::" + t;
        }

        private void traverse(Set<AtlasEntity> seen, AtlasEntity s, List<Link> links, Map<String, Integer> nodeIndices, Map<String, List<AtlasEntity>> outgoingEntities) {

            // To avoid cyclic links.
            if (seen.contains(s)) {
                return;
            }
            seen.add(s);

            // Traverse entities those are updated by this entity.
            final Object outputs = s.getAttribute(NiFiTypes.ATTR_OUTPUTS);
            if (outputs != null) {
                for (Map<String, Object> output : ((List<Map<String, Object>>) outputs)) {
                    final String qname = ((Map<String, String>) output.get("uniqueAttributes")).get("qualifiedName");
                    final AtlasEntity t = atlasEntities.get(toEntityKey((String) output.get("typeName"), qname));

                    if (t != null) {
                        links.add(toLink(s, t, nodeIndices));
                        traverse(seen, t, links, nodeIndices, outgoingEntities);
                    }
                }
            }

            // Add link to the input objects for this entity.
            final Object inputs = s.getAttribute(NiFiTypes.ATTR_INPUTS);
            if (inputs != null) {
                for (Map<String, Object> input : ((List<Map<String, Object>>) inputs)) {
                    final String qname = ((Map<String, String>) input.get("uniqueAttributes")).get("qualifiedName");
                    final AtlasEntity t = atlasEntities.get(toEntityKey((String) input.get("typeName"), qname));

                    if (t != null) {
                        links.add(toLink(t, s, nodeIndices));
                    }
                }
            }


            // Traverse entities those consume this entity as their input.
            final List<AtlasEntity> outGoings = outgoingEntities.get(toEntityKey(s));
            if (outGoings != null) {
                outGoings.forEach(o -> {
                    links.add(toLink(s, o, nodeIndices));
                    traverse(seen, o, links, nodeIndices, outgoingEntities);
                });
            }

        }

        @SuppressWarnings("unchecked")
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            final Lineage result = new Lineage();
            final List<Node> nodes = new ArrayList<>();
            final List<Link> links = new ArrayList<>();
            final Map<String, Integer> nodeIndices = new HashMap<>();
            // DataSet to outgoing Processes.
            final Map<String, List<AtlasEntity>> outgoingEntities = new HashMap<>();
            result.setNodes(nodes);

            // Add all nodes.
            atlasEntities.entrySet().forEach(entry -> {
                nodeIndices.put(entry.getKey(), nodes.size());
                final AtlasEntity entity = entry.getValue();
                nodes.add(toNode(entity));

                // Capture inputs
                final Object inputs = entity.getAttribute(NiFiTypes.ATTR_INPUTS);
                if (inputs != null) {
                    for (Map<String, Object> input : ((List<Map<String, Object>>) inputs)) {
                        final String qname = ((Map<String, String>) input.get("uniqueAttributes")).get("qualifiedName");
                        final String inputKey = toEntityKey((String) input.get("typeName"), qname);
                        final AtlasEntity t = atlasEntities.get(inputKey);
                        if (t != null) {
                            outgoingEntities.computeIfAbsent(inputKey, k -> new ArrayList<>()).add(entity);
                        }

                    }
                }

            });

            // Correct all flow_path
            final Map<String, List<AtlasEntity>> entities = atlasEntities.values().stream()
                    .collect(Collectors.groupingBy(AtlasEntity::getTypeName));
            final HashSet<AtlasEntity> seen = new HashSet<>();

            // Add nifi_flow
            if (entities.containsKey(NiFiTypes.TYPE_NIFI_FLOW)) {
                final Map<String, AtlasEntity> nifiFlows = entities.get(NiFiTypes.TYPE_NIFI_FLOW)
                        .stream().collect(Collectors.toMap(n -> (String) n.getAttribute("qualifiedName"), n -> n));

                if (entities.containsKey(NiFiTypes.TYPE_NIFI_FLOW_PATH)) {

                    final List<AtlasEntity> flowPaths = entities.get(NiFiTypes.TYPE_NIFI_FLOW_PATH);

                    // Find the starting flow_paths
                    final List<AtlasEntity> heads = flowPaths.stream()
                            .filter(p -> {
                                Object inputs = p.getAttribute(NiFiTypes.ATTR_INPUTS);
                                return inputs == null || ((Collection) inputs).isEmpty()
                                        // This condition matches the head processor but has some inputs those are created by notification.
                                        || ((Collection<Map<String, Object>>) inputs).stream().anyMatch(m -> !"nifi_queue".equals(m.get("typeName")));
                            })
                            .collect(Collectors.toList());

                    final List<AtlasEntity> inputPorts = entities.get(NiFiTypes.TYPE_NIFI_INPUT_PORT);
                    if (inputPorts != null) {
                        heads.addAll(inputPorts);
                    }

                    heads.forEach(s -> {

                        // Link it to parent NiFi Flow.
                        final Object nifiFlowRef = s.getAttribute("nifiFlow");
                        if (nifiFlowRef != null) {
                            Map<String, Object> uniqueAttrs = (Map<String, Object>)((Map<String, Object>) nifiFlowRef).get("uniqueAttributes");
                            String qname = (String) uniqueAttrs.get("qualifiedName");

                            final AtlasEntity nifiFlow = nifiFlows.get(qname);
                            if (nifiFlow != null) {
                                links.add(toLink(nifiFlow, s, nodeIndices));
                            }
                        }

                        // Traverse recursively
                        traverse(seen, s, links, nodeIndices, outgoingEntities);
                    });


                }
            }

            final List<Link> uniqueLinks = new ArrayList<>();
            final Set linkKeys = new HashSet<>();
            for (Link link : links) {
                final String linkKey = toLinkKey(link.getSource(), link.getTarget());
                if (!linkKeys.contains(linkKey)) {
                    uniqueLinks.add(link);
                    linkKeys.add(linkKey);
                }
            }

            // Group links by its target, and configure each weight value.
            // E.g. 1 -> 3 and 2 -> 3, then 1 (0.5) -> 3 and 2 (0.5) -> 3.
            uniqueLinks.stream().collect(Collectors.groupingBy(l -> l.getTarget()))
                    .forEach((t, ls) -> ls.forEach(l -> l.setValue(1 / (double) ls.size())));

            result.setLinks(uniqueLinks);

            respondWithJson(resp, result);
        }
    }

}
