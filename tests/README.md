accsyn test environment
=======================


Compute tests:
--------------


Prerequisites
**************

 1. Install and license Nuke 13 on main Mac dev machine and vritual Linux machine "hyperion" (site: linux)
 2. Make sure the test nuke script is available at /Volumes/projects/nuke.
 3. Have dev accsyn up and running, and dev daemon running at Mac and Linux machine.
 4. Assign nuke-13 app to machines.
 5. Copy the fake render script (fake_render_nuke_v001.py) to /usr/local/Nuke13.1v2/Nuke13.1 and chmod 755 it.
 

API test
********

Launch the API:
    
    import accsyn_api;session = accsyn_api.Session(dev=True)
    
Submit Nuke render that spans over hq and linux site:

    jobs = session.create("job", open("/Users/henriknorin/Documents/accsyn/dev/github/compute-scripts/tests/nuke-13.json", "r").read())
    

 

