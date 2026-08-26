add_test( [==[the matcher never asks the allocator once the session is running]==] /Users/louisgrennell/Exchange/build-sanitize/components/matcher/matcher-allocation [==[the matcher never asks the allocator once the session is running]==]  )
set_tests_properties( [==[the matcher never asks the allocator once the session is running]==] PROPERTIES WORKING_DIRECTORY /Users/louisgrennell/Exchange/build-sanitize/components/matcher SKIP_RETURN_CODE 4)
set( matcher-allocation_TESTS [==[the matcher never asks the allocator once the session is running]==])
