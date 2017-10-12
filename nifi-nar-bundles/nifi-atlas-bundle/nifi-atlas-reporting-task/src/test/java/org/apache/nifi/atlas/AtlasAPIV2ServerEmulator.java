package org.apache.nifi.atlas;

import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Emulate Atlas API v2 server for NiFi implementation testing.
 */
public class AtlasAPIV2ServerEmulator {

    private static final Logger logger = LoggerFactory.getLogger(AtlasAPIV2ServerEmulator.class);
    private static Server server;
    private static ServerConnector httpConnector;

    public static void main(String[] args) throws Exception {
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
        server.start();

        logger.info("Starting {} on port {}", AtlasAPIV2ServerEmulator.class.getSimpleName(), httpConnector.getLocalPort());
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

    private static String toEntityKey(AtlasEntity entity) {
        return toEntityKey(entity.getTypeName(), (String) entity.getAttribute("qualifiedName"));
    }

    private static String toEntityKey(String typeName, String qName) {
        return typeName + "::" + qName;
    }

    public static class LineageServlet extends HttpServlet {

        private Map<String, String> toNode(AtlasEntity entity) {
            Map<String, String> node = new HashMap<>();
            node.put("name", entity.getAttribute(NiFiTypes.ATTR_NAME).toString());
            return node;
        }

        private Map<String, Integer> toLink(AtlasEntity s, AtlasEntity t, Map<String, Integer> nodeIndices) {
            final Integer sid = nodeIndices.get(toEntityKey(s));
            final Integer tid = nodeIndices.get(toEntityKey(t));

            Map<String, Integer> link = new HashMap<>();
            link.put("source", sid);
            link.put("target", tid);
            link.put("value", 1);
            return link;
        }

        private void traverse(AtlasEntity s, List<Map<String, Integer>> links, Map<String, Integer> nodeIndices, Map<String, List<AtlasEntity>> outgoingEntities) {
            // Traverse entities those are updated by this.
            final Object outputs = s.getAttribute(NiFiTypes.ATTR_OUTPUTS);
            if (outputs != null) {
                for (Map<String, Object> output : ((List<Map<String, Object>>) outputs)) {
                    final String qname = ((Map<String, String>) output.get("uniqueAttributes")).get("qualifiedName");
                    final AtlasEntity t = atlasEntities.get(toEntityKey((String) output.get("typeName"), qname));

                    if (t != null) {
                        links.add(toLink(s, t, nodeIndices));
                        traverse(t, links, nodeIndices, outgoingEntities);
                    }
                }
            }

            // Traverse entities those consume this as their input.
            final List<AtlasEntity> outGoings = outgoingEntities.get(toEntityKey(s));
            if (outGoings != null) {
                outGoings.forEach(o -> {
                    links.add(toLink(s, o, nodeIndices));
                    traverse(o, links, nodeIndices, outgoingEntities);
                });
            }

        }

        @SuppressWarnings("unchecked")
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            final Map<String, Object> result = new HashMap<>();
            final List<Map<String, String>> nodes = new ArrayList<>();
            final List<Map<String, Integer>> links = new ArrayList<>();
            final Map<String, Integer> nodeIndices = new HashMap<>();
            // DataSet to outgoing Processes.
            final Map<String, List<AtlasEntity>> outgoingEntities = new HashMap<>();
            result.put("nodes", nodes);
            result.put("links", links);

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

            // Add nifi_flow
            if (entities.containsKey(NiFiTypes.TYPE_NIFI_FLOW)) {
                AtlasEntity nifiFlow = entities.get(NiFiTypes.TYPE_NIFI_FLOW).get(0);

                if (entities.containsKey(NiFiTypes.TYPE_NIFI_FLOW_PATH)) {

                    final List<AtlasEntity> flowPaths = entities.get(NiFiTypes.TYPE_NIFI_FLOW_PATH);

                    // Find the starting flow_paths
                    final List<AtlasEntity> heads = flowPaths.stream()
                            .filter(p -> {
                                Object inputs = p.getAttribute(NiFiTypes.ATTR_INPUTS);
                                return inputs == null || ((Collection) inputs).isEmpty();
                            })
                            .collect(Collectors.toList());

                    final List<AtlasEntity> inputPorts = entities.get(NiFiTypes.TYPE_NIFI_INPUT_PORT);
                    if (inputPorts != null) {
                        heads.addAll(inputPorts);
                    }

                    // Search recursively
                    heads.forEach(s -> {
                        links.add(toLink(nifiFlow, s, nodeIndices));
                        traverse(s, links, nodeIndices, outgoingEntities);
                    });


                }
            }


            respondWithJson(resp, result);
        }
    }

}
