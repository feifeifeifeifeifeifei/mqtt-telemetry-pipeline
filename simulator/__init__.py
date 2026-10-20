"""A deterministic MQTT gateway that replays a scripted day and a rotating fault catalogue.

Stands in for real hardware in the demo stack. It never imports the service:
the only link between the two is the contract the tests check, namely that
every frame gets exactly the verdict its fault catalogue entry promises.
"""
