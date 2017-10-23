package org.apache.nifi.atlas.emulator;

import org.junit.Assert;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class Lineage {

    private List<Node> nodes;
    private List<Link> links;

    public List<Node> getNodes() {
        return nodes;
    }

    public void setNodes(List<Node> nodes) {
        this.nodes = nodes;
    }

    public List<Link> getLinks() {
        return links;
    }

    public void setLinks(List<Link> links) {
        this.links = links;
    }

    public Node findNode(String type, String qname) {
        return nodes.stream().filter(n -> type.equals(n.getType()) && qname.equals(n.getQualifiedName()))
                .findFirst().orElseGet(() -> {
            Assert.fail(String.format("Node was not found for %s::%s", type, qname));
            return null;
        });
    }

    public Node findNode(String type, String name, String qname) {
        final Node node = findNode(type, qname);
        assertEquals(name, node.getName());
        return node;
    }

    public int getNodeIndex(String type, String qname) {
        for (int i = 0; i < nodes.size(); i++) {
            Node n = nodes.get(i);
            if (type.equals(n.getType()) && qname.equals(n.getQualifiedName())) {
                return i;
            }
        }
        return -1;
    }

    public void assertLink(Node s, Node t) {
        assertLink(s.getType(), s.getName(), s.getQualifiedName(), t.getType(), t.getName(), t.getQualifiedName());
    }

    public void assertLink(String sType, String sName, String sQname, String tType, String tName, String tQname) {
        int si = getNodeIndex(sType, sQname);
        assertTrue(String.format("Source node was not found for %s::%s", sType, sQname), si > -1);
        int ti = getNodeIndex(tType, tQname);
        assertTrue(String.format("Target node was not found for %s::%s", tType, tQname), ti > -1);

        assertNotNull(findNode(sType, sName, sQname));
        assertNotNull(findNode(tType, tName, tQname));

        assertTrue(String.format("Link from %s::%s to %s::%s was not found", sType, sQname, tType, tQname),
                links.stream().anyMatch(l -> l.getSource() == si && l.getTarget() == ti));

    }
}
