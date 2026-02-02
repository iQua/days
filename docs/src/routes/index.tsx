import { createFileRoute, Link } from '@tanstack/react-router';
import { HomeLayout } from 'fumadocs-ui/layouts/home';
import { baseOptions } from '@/lib/layout.shared';

export const Route = createFileRoute('/')({
  component: Home,
});

function Home() {
  return (
    <HomeLayout {...baseOptions()}>
      <div className="flex flex-col flex-1 justify-center px-4 py-8 text-center">
        <h1 className="font-semibold text-3xl mb-3">Days</h1>
        <p className="text-fd-muted-foreground max-w-xl mx-auto mb-6">
          A performant discrete-event network simulator in Rust, with optional trace-based conformance
          checking via LeanGuard.
        </p>
        <div className="flex gap-3 justify-center">
          <Link
            to="/docs/$"
            params={{
              _splat: '',
            }}
            className="px-3 py-2 rounded-lg bg-fd-primary text-fd-primary-foreground font-medium text-sm"
          >
            Open Docs
          </Link>
          <a
            href="https://github.com/iqua/days"
            className="px-3 py-2 rounded-lg border border-fd-border text-fd-foreground font-medium text-sm"
          >
            GitHub
          </a>
        </div>
      </div>
    </HomeLayout>
  );
}
